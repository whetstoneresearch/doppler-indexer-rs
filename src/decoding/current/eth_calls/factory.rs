//! Factory / once-call message handling for live mode.

use std::collections::HashSet;
use std::fs::File;
use std::path::Path;

use arrow::array::{Array, FixedSizeBinaryArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tokio::sync::mpsc::Sender;

use crate::decoding::eth_calls::column_index::{
    load_or_build_decoded_column_index, read_once_calls_from_parquet,
    read_raw_parquet_function_names,
};
use crate::decoding::eth_calls::{
    build_result_map, decode_value, process_once_calls, CallDecodeConfig, EthCallDecodingError,
};
use crate::live::{LiveDecodedOnceCall, LiveStorage};
use crate::transformations::{DecodedCall as TransformDecodedCall, DecodedCallsMessage};
use crate::types::decoded::DecodedValue;

fn read_parquet_address_set(path: &Path) -> Result<HashSet<[u8; 20]>, EthCallDecodingError> {
    let file = File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut addresses = HashSet::new();

    for batch in reader {
        let batch = batch?;
        let address_idx = batch.schema().index_of("contract_address").map_err(|e| {
            EthCallDecodingError::Decode(format!("Missing contract_address: {}", e))
        })?;
        let address_arr = batch
            .column(address_idx)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| {
                EthCallDecodingError::Decode(
                    "contract_address column is not FixedSizeBinaryArray".to_string(),
                )
            })?;

        for row in 0..address_arr.len() {
            let bytes = address_arr.value(row);
            if bytes.len() != 20 {
                return Err(EthCallDecodingError::Decode(
                    "contract_address column must contain 20-byte values".to_string(),
                ));
            }
            let mut address = [0u8; 20];
            address.copy_from_slice(bytes);
            addresses.insert(address);
        }
    }

    Ok(addresses)
}

/// Handle a live-mode `OnceCallsReady` message: decode results, persist to bincode,
/// and optionally forward to the transformation engine.
#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_once_calls_live(
    live_storage: &LiveStorage,
    range_start: u64,
    range_end: u64,
    contract_name: &str,
    results: &[crate::decoding::OnceCallResult],
    configs: &[&CallDecodeConfig],
    transform_tx: Option<&Sender<DecodedCallsMessage>>,
    retry_transform_after_decode: bool,
) -> Result<(), EthCallDecodingError> {
    let mut decoded_once_calls: Vec<LiveDecodedOnceCall> = Vec::with_capacity(results.len());
    // Group transform calls by function name
    let mut transform_calls_by_fn: std::collections::HashMap<String, Vec<TransformDecodedCall>> =
        std::collections::HashMap::new();

    for result in results {
        let mut decoded_values: Vec<(String, DecodedValue)> = Vec::new();

        for config in configs {
            if let Some(raw_value) = result.results.get(&config.function_name) {
                match decode_value(raw_value, &config.output_type) {
                    Ok(decoded) => {
                        let transform_call = TransformDecodedCall {
                            block_number: result.block_number,
                            block_timestamp: result.block_timestamp,
                            contract_address: result.contract_address,
                            source_name: contract_name.to_string(),
                            function_name: config.function_name.clone(),
                            trigger_log_index: None,
                            result: build_result_map(
                                &decoded,
                                &config.output_type,
                                &config.function_name,
                            ),
                            is_reverted: false,
                            revert_reason: None,
                        };
                        transform_calls_by_fn
                            .entry(config.function_name.clone())
                            .or_default()
                            .push(transform_call);

                        decoded_values.push((config.function_name.clone(), decoded.clone()));
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
        if let Err(e) =
            live_storage.write_decoded_once_calls(range_start, contract_name, &decoded_once_calls)
        {
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
                    source_name: contract_name.to_string(),
                    function_name,
                    calls,
                };
                let _ = tx.send(msg).await;
            }
        }
    }

    Ok(())
}

/// Handle an `OnceFileBackfilled` message: decode missing columns, or fully
/// rewrite the decoded file if the raw file now contains a different address set.
pub(super) async fn handle_once_file_backfilled(
    raw_calls_dir: &Path,
    output_base: &Path,
    range_start: u64,
    range_end: u64,
    contract_name: &str,
    once_configs: &[CallDecodeConfig],
    transform_tx: Option<&Sender<DecodedCallsMessage>>,
) -> Result<(), EthCallDecodingError> {
    let configs: Vec<&CallDecodeConfig> = once_configs
        .iter()
        .filter(|c| c.contract_name == contract_name)
        .collect();

    if configs.is_empty() {
        return Ok(());
    }

    let file_name = format!("{}-{}.parquet", range_start, range_end - 1);
    let raw_path = raw_calls_dir
        .join(contract_name)
        .join("once")
        .join(&file_name);

    if !raw_path.exists() {
        return Ok(());
    }

    // Read raw columns from parquet
    let raw_cols: HashSet<String> = read_raw_parquet_function_names(&raw_path);

    // Read decoded columns from index or parquet
    let decoded_dir = output_base.join(contract_name).join("once");
    let raw_once_dir = raw_calls_dir.join(contract_name).join("once");
    let decoded_index = load_or_build_decoded_column_index(&decoded_dir, &raw_once_dir);
    let decoded_path = decoded_dir.join(&file_name);
    let decoded_cols: HashSet<String> = decoded_index
        .get(&file_name)
        .map(|cols| cols.iter().cloned().collect())
        .unwrap_or_default();

    let needs_full_redecode = if decoded_path.exists() {
        read_parquet_address_set(&raw_path)? != read_parquet_address_set(&decoded_path)?
    } else {
        false
    };

    // Find configs that need decoding
    let configs_to_decode: Vec<CallDecodeConfig> = if needs_full_redecode {
        configs.iter().cloned().cloned().collect()
    } else {
        configs
            .iter()
            .filter(|c| {
                raw_cols.contains(&c.function_name) && !decoded_cols.contains(&c.function_name)
            })
            .cloned()
            .cloned()
            .collect()
    };

    if !configs_to_decode.is_empty() {
        if needs_full_redecode {
            tracing::info!(
                "OnceFileBackfilled: re-decoding full file for {}/{} because the raw address set changed",
                contract_name,
                file_name
            );
        } else {
            tracing::info!(
                "OnceFileBackfilled: decoding {} new columns for {}/{}",
                configs_to_decode.len(),
                contract_name,
                file_name
            );
        }

        let configs_ref: Vec<&CallDecodeConfig> = configs_to_decode.iter().collect();
        let results = read_once_calls_from_parquet(&raw_path, &configs_ref)?;
        process_once_calls(
            &results,
            range_start,
            range_end,
            contract_name,
            &configs_ref,
            output_base,
            transform_tx,
            false,
            needs_full_redecode,
        )
        .await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use alloy::dyn_abi::DynSolValue;
    use tempfile::TempDir;

    use crate::decoding::eth_calls::column_index::read_decoded_parquet_function_names;
    use crate::raw_data::historical::eth_calls::{
        write_once_results_to_parquet, OnceCallResult as RawOnceCallResult,
    };
    use crate::types::config::eth_call::EvmType;

    fn test_config(contract_name: &str) -> CallDecodeConfig {
        CallDecodeConfig {
            contract_name: contract_name.to_string(),
            function_name: "name".to_string(),
            output_type: EvmType::String,
            _is_once: true,
            start_block: None,
        }
    }

    #[tokio::test]
    async fn test_once_file_backfilled_rewrites_when_raw_has_new_addresses() {
        let tmp = TempDir::new().unwrap();
        let raw_base = tmp.path().join("raw");
        let decoded_base = tmp.path().join("decoded");
        let contract_name = "DERC20";
        let config = test_config(contract_name);
        let file_name = "0-99.parquet";

        let raw_dir = raw_base.join(contract_name).join("once");
        std::fs::create_dir_all(&raw_dir).unwrap();
        let raw_path = raw_dir.join(file_name);

        let mut results = HashMap::new();
        results.insert(
            "name".to_string(),
            DynSolValue::String("token".to_string()).abi_encode(),
        );
        write_once_results_to_parquet(
            &[RawOnceCallResult {
                block_number: 1,
                block_timestamp: 10,
                contract_address: [0x11; 20],
                function_results: results,
            }],
            &raw_path,
            &[String::from("name")],
        )
        .unwrap();

        process_once_calls(
            &[],
            0,
            100,
            contract_name,
            &[&config],
            &decoded_base,
            None,
            false,
            false,
        )
        .await
        .unwrap();

        handle_once_file_backfilled(
            &raw_base,
            &decoded_base,
            0,
            100,
            contract_name,
            &[config],
            None,
        )
        .await
        .unwrap();

        let decoded_path = decoded_base
            .join(contract_name)
            .join("once")
            .join(file_name);
        let decoded_addrs = read_parquet_address_set(&decoded_path).unwrap();
        assert_eq!(
            decoded_addrs.len(),
            1,
            "decoded once file should be fully rewritten"
        );
        assert!(
            decoded_addrs.contains(&[0x11; 20]),
            "decoded once file should contain the raw address after rewrite"
        );
        assert!(
            read_decoded_parquet_function_names(&decoded_path).contains("name"),
            "decoded once file should preserve the once column set"
        );
    }
}
