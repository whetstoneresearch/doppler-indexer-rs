//! Shared post-processing helpers for eth_call results: parquet write, S3 upload, decoder notify, frequency update.

use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;

use super::parquet_io::write_results_to_parquet_async;
use super::types::{CallResult, EthCallCollectionError, FrequencyState};
use crate::decoding::{DecoderMessage, EthCallResult as DecoderEthCallResult};
use crate::storage::{upload_parquet_to_s3, StorageManager};
use crate::types::config::eth_call::Frequency;

/// Describes how to finalize a batch of regular (non-once) eth_call results:
/// write parquet, optionally bench, log, upload to S3, notify decoder, update frequency.
pub(crate) struct FinalizeRegularParams<'a> {
    /// The call results to write (already sorted).
    pub results: Vec<CallResult>,
    /// Max param columns in the parquet schema.
    pub max_params: usize,
    /// Output directory for this contract/function.
    pub sub_dir: PathBuf,
    /// Parquet file name (e.g. "1000_1999.parquet").
    pub file_name: String,
    /// Human-readable label for logging (e.g. "factory eth_call", "eth_call", "multicall token").
    pub log_label: String,
    /// S3 data type path (e.g. "raw/eth_calls/MyContract/myFunc").
    pub s3_data_type: String,
    /// Chain name for S3 upload.
    pub chain_name: &'a str,
    /// Range start block.
    pub range_start: u64,
    /// Range end block (exclusive).
    pub range_end: u64,
    /// Optional storage manager for S3 upload.
    pub storage_manager: Option<&'a Arc<StorageManager>>,
    /// Optional decoder sender.
    pub decoder_tx: &'a Option<Sender<DecoderMessage>>,
    /// Contract name for the decoder message.
    pub contract_name: String,
    /// Function name for the decoder message.
    pub function_name: String,
    /// The frequency of this call (for updating frequency state).
    pub frequency: &'a Frequency,
    /// Frequency state to update.
    pub frequency_state: &'a mut FrequencyState,
    /// Timestamp of the last block in the filtered set (for frequency update).
    pub last_block_timestamp: Option<u64>,
    /// Optional bench context: (bench_label, rpc_time, process_time).
    #[cfg(feature = "bench")]
    pub bench: Option<(String, std::time::Duration, std::time::Duration)>,
}

/// Finalize a batch of regular (non-once) eth_call results.
///
/// This consolidates the repeated post-processing pattern:
/// 1. Build decoder results (if decoder_tx is Some)
/// 2. spawn_blocking parquet write
/// 3. Bench recording (if bench feature enabled)
/// 4. Log the result count
/// 5. S3 upload (if storage_manager is Some)
/// 6. Decoder message send (if decoder_tx is Some)
/// 7. Frequency state update (if frequency is Duration)
pub(crate) async fn finalize_regular_results(
    params: FinalizeRegularParams<'_>,
) -> Result<(), EthCallCollectionError> {
    let result_count = params.results.len();
    let output_path = params.sub_dir.join(&params.file_name);

    // Build decoder results before moving results into spawn_blocking
    let decoder_results: Option<Vec<DecoderEthCallResult>> = if params.decoder_tx.is_some() {
        Some(
            params
                .results
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

    // 1. Parquet write via async wrapper
    let max_params = params.max_params;
    let results = params.results;
    #[cfg(feature = "bench")]
    let write_start = std::time::Instant::now();
    write_results_to_parquet_async(results, output_path.clone(), max_params).await?;

    // 2. Bench recording
    #[cfg(feature = "bench")]
    {
        let write_time = write_start.elapsed();
        if let Some((bench_label, rpc_time, process_time)) = params.bench {
            crate::bench::record(
                &bench_label,
                params.range_start,
                params.range_end,
                result_count,
                rpc_time,
                process_time,
                write_time,
            );
        }
    }

    // 3. Log
    let output_path_for_upload = params.sub_dir.join(&params.file_name);
    tracing::info!(
        "Wrote {} {} results to {}",
        result_count,
        params.log_label,
        output_path_for_upload.display()
    );

    // 4. S3 upload
    if let Some(sm) = params.storage_manager {
        upload_parquet_to_s3(
            sm,
            &output_path_for_upload,
            params.chain_name,
            &params.s3_data_type,
            params.range_start,
            params.range_end - 1,
        )
        .await
        .map_err(|e| EthCallCollectionError::Io(std::io::Error::other(e.to_string())))?;
    }

    // 5. Decoder notification
    // Clone names before potential move into decoder message, for frequency update below
    let contract_name = params.contract_name;
    let function_name = params.function_name;
    if let Some(tx) = params.decoder_tx {
        if let Some(results) = decoder_results {
            let _ = tx
                .send(DecoderMessage::EthCallsReady {
                    range_start: params.range_start,
                    range_end: params.range_end,
                    contract_name: contract_name.clone(),
                    function_name: function_name.clone(),
                    results,
                    live_mode: false,
                    retry_transform_after_decode: false,
                })
                .await;
        }
    }

    // 6. Frequency state update
    if let Frequency::Duration(_) = params.frequency {
        if let Some(ts) = params.last_block_timestamp {
            params
                .frequency_state
                .last_call_times
                .insert((contract_name, function_name), ts);
        }
    }

    Ok(())
}

/// Deferred version of [`finalize_regular_results`].
///
/// Performs the frequency state update synchronously (needed for correct
/// frequency filtering on subsequent ranges), but spawns the I/O work
/// (parquet write, S3 upload, decoder notification) as a background tokio
/// task. Returns the `JoinHandle` so the caller can await it later (e.g.
/// at the start of the next range iteration) to pipeline writes behind RPC.
pub(crate) async fn finalize_regular_results_deferred(
    params: FinalizeRegularParams<'_>,
) -> Result<JoinHandle<Result<(), EthCallCollectionError>>, EthCallCollectionError> {
    let result_count = params.results.len();
    let output_path = params.sub_dir.join(&params.file_name);

    // Build decoder results before moving results into the spawned task
    let decoder_results: Option<Vec<DecoderEthCallResult>> = if params.decoder_tx.is_some() {
        Some(
            params
                .results
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

    // Clone owned copies of everything the background task needs
    let results = params.results;
    let max_params = params.max_params;
    let log_label = params.log_label;
    let s3_data_type = params.s3_data_type;
    let chain_name = params.chain_name.to_string();
    let range_start = params.range_start;
    let range_end = params.range_end;
    let storage_manager = params.storage_manager.cloned();
    let decoder_tx = params.decoder_tx.clone();
    let contract_name = params.contract_name;
    let function_name = params.function_name;
    let file_name = params.file_name;
    let sub_dir = params.sub_dir;

    #[cfg(feature = "bench")]
    let bench = params.bench;

    // Frequency state update happens synchronously (before spawn) so the
    // next range sees the updated state.
    if let Frequency::Duration(_) = params.frequency {
        if let Some(ts) = params.last_block_timestamp {
            params
                .frequency_state
                .last_call_times
                .insert((contract_name.clone(), function_name.clone()), ts);
        }
    }

    let handle = tokio::spawn(async move {
        // 1. Parquet write
        #[cfg(feature = "bench")]
        let write_start = std::time::Instant::now();
        write_results_to_parquet_async(results, output_path.clone(), max_params).await?;

        // 2. Bench recording
        #[cfg(feature = "bench")]
        {
            let write_time = write_start.elapsed();
            if let Some((bench_label, rpc_time, process_time)) = bench {
                crate::bench::record(
                    &bench_label,
                    range_start,
                    range_end,
                    result_count,
                    rpc_time,
                    process_time,
                    write_time,
                );
            }
        }

        // 3. Log
        let output_path_for_upload = sub_dir.join(&file_name);
        tracing::info!(
            "Wrote {} {} results to {}",
            result_count,
            log_label,
            output_path_for_upload.display()
        );

        // 4. S3 upload
        if let Some(sm) = &storage_manager {
            upload_parquet_to_s3(
                sm,
                &output_path_for_upload,
                &chain_name,
                &s3_data_type,
                range_start,
                range_end - 1,
            )
            .await
            .map_err(|e| EthCallCollectionError::Io(std::io::Error::other(e.to_string())))?;
        }

        // 5. Decoder notification
        if let Some(tx) = &decoder_tx {
            if let Some(results) = decoder_results {
                let _ = tx
                    .send(DecoderMessage::EthCallsReady {
                        range_start,
                        range_end,
                        contract_name: contract_name.clone(),
                        function_name: function_name.clone(),
                        results,
                        live_mode: false,
                        retry_transform_after_decode: false,
                    })
                    .await;
            }
        }

        Ok(())
    });

    Ok(handle)
}
