pub(crate) mod column_index;
pub(crate) mod config;
pub(crate) mod decode;
pub(crate) mod parquet_io;
pub(crate) mod process;
pub(crate) mod transform;
pub(crate) mod types;

pub use types::*;

pub use config::build_decode_configs;
pub use decode::decode_value;
pub use process::{process_event_calls, process_once_calls, process_regular_calls};
pub use transform::build_result_map;

use std::path::PathBuf;
use std::time::Instant;

use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;

use super::catchup;
use super::current;
use crate::live::TransformRetryRequest;
use crate::transformations::{
    DecodedCallsMessage, RangeCompleteMessage,
};
use crate::types::config::chain::ChainConfig;
use crate::types::config::raw_data::RawDataCollectionConfig;

pub async fn decode_eth_calls(
    chain: &ChainConfig,
    raw_data_config: &RawDataCollectionConfig,
    decoder_rx: Receiver<super::types::DecoderMessage>,
    transform_tx: Option<Sender<DecodedCallsMessage>>,
    complete_tx: Option<Sender<RangeCompleteMessage>>,
    transform_retry_tx: Option<Sender<TransformRetryRequest>>,
    eth_calls_catchup_done_rx: Option<oneshot::Receiver<()>>,
    decode_catchup_done_tx: Option<oneshot::Sender<()>>,
    skip_catchup: bool,
) -> Result<(), EthCallDecodingError> {
    let output_base = PathBuf::from(format!("data/{}/historical/decoded/eth_calls", chain.name));
    std::fs::create_dir_all(&output_base)?;

    // Build decode configs from contract configurations
    let (regular_configs, once_configs, event_configs) = build_decode_configs(&chain.contracts);

    if regular_configs.is_empty() && once_configs.is_empty() && event_configs.is_empty() {
        tracing::info!(
            "No eth_calls configured for decoding on chain {}",
            chain.name
        );
        // Signal barrier before early return so the engine doesn't wait forever
        if let Some(tx) = decode_catchup_done_tx {
            let _ = tx.send(());
        }
        // Drain channel and return
        let mut decoder_rx = decoder_rx;
        while decoder_rx.recv().await.is_some() {}
        return Ok(());
    }

    tracing::info!(
        "Eth_call decoder starting for chain {} with {} regular configs, {} once configs, {} event configs",
        chain.name,
        regular_configs.len(),
        once_configs.len(),
        event_configs.len()
    );

    let raw_calls_dir = PathBuf::from(format!("data/{}/historical/raw/eth_calls", chain.name));

    if !skip_catchup {
        // =========================================================================
        // Wait for eth_call collection catchup before decoding
        // =========================================================================
        if let Some(rx) = eth_calls_catchup_done_rx {
            tracing::info!(
                "Waiting for eth_call collection catchup to complete before decoding..."
            );
            let _ = rx.await;
            tracing::info!("Eth_call collection catchup complete, proceeding with decoding");
        }

        // =========================================================================
        // Catchup phase: Process existing raw eth_call files
        // =========================================================================
        if raw_calls_dir.exists() {
            let decode_start = Instant::now();
            // Pass None for transform_tx during catchup to avoid deadlock:
            // the engine is blocked waiting for our barrier signal and won't read
            // from the channel, so sends could block forever. The engine will read
            // decoded parquet files during its own catchup instead.
            catchup::catchup_decode_eth_calls(
                &raw_calls_dir,
                &output_base,
                &regular_configs,
                &once_configs,
                &event_configs,
                raw_data_config,
                None,
            )
            .await?;

            tracing::info!(
                "Eth_call decoding catchup complete for chain {} in {:.1}s",
                chain.name,
                decode_start.elapsed().as_secs_f64()
            );
        } else {
            tracing::info!(
                "Eth_call decoding catchup complete for chain {} (no raw files)",
                chain.name
            );
        }

        if let Some(tx) = decode_catchup_done_tx {
            let _ = tx.send(());
        }
    }

    // =========================================================================
    // Live phase: Process new data as it arrives
    // =========================================================================
    current::decode_eth_calls_live(
        decoder_rx,
        &raw_calls_dir,
        &output_base,
        &chain.name,
        &regular_configs,
        &once_configs,
        &event_configs,
        raw_data_config,
        transform_tx.as_ref(),
        complete_tx.as_ref(),
        transform_retry_tx.as_ref(),
    )
    .await?;

    tracing::info!("Eth_call decoding complete for chain {}", chain.name);
    Ok(())
}
