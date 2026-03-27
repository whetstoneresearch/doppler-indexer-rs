//! Shared helpers for the eth_call finalization pipeline:
//! sort → write parquet → S3 upload → decoder message.
//!
//! Also contains the frequency filter + state update helper and a small
//! utility for extracting a function name from a Solidity signature.

use super::parquet_io::write_results_to_parquet;
use super::types::{
    BlockInfo, BlockRange, CallResult, EthCallCollectionError, EthCallContext, FrequencyState,
};
use crate::decoding::{DecoderMessage, EthCallResult as DecoderEthCallResult};
use crate::storage::upload_parquet_to_s3;
use crate::types::config::eth_call::Frequency;

use super::frequency::filter_blocks_for_frequency;

/// The finalization pipeline that repeats across all process_* functions for
/// regular (non-once) call results:
///
/// 1. Sort results by (block_number, contract_address, param_values)
/// 2. Write to parquet via `spawn_blocking`
/// 3. Upload to S3 if configured
/// 4. Send decoder message if channel is configured
#[allow(dead_code)]
pub(crate) async fn finalize_call_results(
    results: &mut Vec<CallResult>,
    ctx: &EthCallContext<'_>,
    range: &BlockRange,
    contract_name: &str,
    function_name: &str,
    max_params: usize,
    #[cfg(feature = "bench")] bench_label: Option<&str>,
    #[cfg(feature = "bench")] rpc_time: std::time::Duration,
    #[cfg(feature = "bench")] process_time: std::time::Duration,
) -> Result<usize, EthCallCollectionError> {
    results.sort_by_key(|r| (r.block_number, r.contract_address, r.param_values.clone()));

    let result_count = results.len();
    let sub_dir = ctx.output_dir.join(contract_name).join(function_name);
    std::fs::create_dir_all(&sub_dir)?;
    let file_name = range.file_name("");
    let output_path = sub_dir.join(&file_name);

    let decoder_results: Option<Vec<DecoderEthCallResult>> = if ctx.decoder_tx.is_some() {
        Some(
            results
                .iter()
                .map(|r| DecoderEthCallResult {
                    block_number: r.block_number,
                    block_timestamp: r.block_timestamp,
                    contract_address: r.contract_address,
                    value: r.value_bytes.clone(),
                })
                .collect(),
        )
    } else {
        None
    };

    #[cfg(feature = "bench")]
    let write_start = std::time::Instant::now();

    let results_owned = std::mem::take(results);
    tokio::task::spawn_blocking(move || {
        write_results_to_parquet(&results_owned, &output_path, max_params)
    })
    .await
    .map_err(|e| EthCallCollectionError::JoinError(e.to_string()))??;

    #[cfg(feature = "bench")]
    {
        if let Some(label) = bench_label {
            let write_time = write_start.elapsed();
            crate::bench::record(label, range.start, range.end, result_count, rpc_time, process_time, write_time);
        }
    }

    let output_path_for_upload = sub_dir.join(&file_name);
    tracing::info!(
        "Wrote {} eth_call results to {}",
        result_count,
        output_path_for_upload.display()
    );

    // Upload to S3 if configured
    if let Some(sm) = ctx.storage_manager {
        let data_type = format!("raw/eth_calls/{}/{}", contract_name, function_name);
        upload_parquet_to_s3(
            sm,
            &output_path_for_upload,
            ctx.chain_name,
            &data_type,
            range.start,
            range.end - 1,
        )
        .await
        .map_err(|e| EthCallCollectionError::Io(std::io::Error::other(e.to_string())))?;
    }

    if let Some(tx) = ctx.decoder_tx {
        if let Some(results) = decoder_results {
            let _ = tx
                .send(DecoderMessage::EthCallsReady {
                    range_start: range.start,
                    range_end: range.end,
                    contract_name: contract_name.to_string(),
                    function_name: function_name.to_string(),
                    results,
                    live_mode: false,
                    retry_transform_after_decode: false,
                })
                .await;
        }
    }

    Ok(result_count)
}

/// Apply frequency filter and update frequency state if the frequency is
/// `Duration`-based.
///
/// Returns the filtered blocks. Caller is expected to skip processing when
/// the returned slice is empty.
#[allow(dead_code)]
pub(crate) fn apply_frequency_filter<'a>(
    blocks: &'a [BlockInfo],
    frequency: &Frequency,
    state_key: &(String, String),
    frequency_state: &mut FrequencyState,
) -> Vec<&'a BlockInfo> {
    let last_call_ts = frequency_state.last_call_times.get(state_key).copied();
    filter_blocks_for_frequency(blocks, frequency, last_call_ts)
}

/// Update frequency state after successfully processing a group of blocks.
/// Only meaningful for `Frequency::Duration` — other variants are no-ops.
#[allow(dead_code)]
pub(crate) fn update_frequency_state(
    frequency: &Frequency,
    filtered_blocks: &[BlockInfo],
    state_key: (String, String),
    frequency_state: &mut FrequencyState,
) {
    if let Frequency::Duration(_) = frequency {
        if let Some(last_block) = filtered_blocks.last() {
            frequency_state
                .last_call_times
                .insert(state_key, last_block.timestamp);
        }
    }
}

/// Update frequency state using borrowed block references.
#[allow(dead_code)]
pub(crate) fn update_frequency_state_ref(
    frequency: &Frequency,
    filtered_blocks: &[&BlockInfo],
    state_key: (String, String),
    frequency_state: &mut FrequencyState,
) {
    if let Frequency::Duration(_) = frequency {
        if let Some(last_block) = filtered_blocks.last() {
            frequency_state
                .last_call_times
                .insert(state_key, last_block.timestamp);
        }
    }
}

/// Extract the function name from a Solidity function signature.
///
/// E.g. `"slot0()"` → `"slot0"`, `"balanceOf(address)"` → `"balanceOf"`.
pub(crate) fn extract_function_name(signature: &str) -> &str {
    signature.split('(').next().unwrap_or(signature)
}
