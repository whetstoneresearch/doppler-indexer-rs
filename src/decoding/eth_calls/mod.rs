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
pub use transform::{build_result_map, build_result_map_for_merge};

use std::time::Instant;

use tokio::sync::mpsc::Receiver;

use super::catchup;
use super::current;
use super::types::EthCallDecoderOutputs;
use crate::types::config::chain::ChainConfig;
use crate::types::config::raw_data::RawDataCollectionConfig;
use crate::types::shared::repair::RepairScope;

#[allow(clippy::too_many_arguments)]
pub async fn decode_eth_calls(
    chain: &ChainConfig,
    raw_data_config: &RawDataCollectionConfig,
    decoder_rx: Receiver<super::types::DecoderMessage>,
    outputs: EthCallDecoderOutputs<'_>,
    skip_catchup: bool,
    repair: bool,
    repair_scope: Option<RepairScope>,
) -> Result<(), EthCallDecodingError> {
    let output_base = crate::storage::paths::decoded_eth_calls_dir(&chain.name);
    std::fs::create_dir_all(&output_base)?;

    // Build decode configs from contract configurations
    let (regular_configs, once_configs, event_configs) = build_decode_configs(&chain.contracts);

    if regular_configs.is_empty() && once_configs.is_empty() && event_configs.is_empty() {
        tracing::info!(
            "No eth_calls configured for decoding on chain {}",
            chain.name
        );
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

    let raw_calls_dir = crate::storage::paths::raw_eth_calls_dir(&chain.name);

    // =========================================================================
    // Catchup (disk-scan) runs concurrently with the live loop.
    //
    // The disk-scan catches up ranges that existed on-disk before this run
    // started (resumability). The live loop drains streaming DecoderMessage
    // messages from the eth_calls catchup (now plumbed with decoder_tx) and
    // from the current phase. Concurrent execution removes the old
    // `eth_calls_catchup_done_rx` barrier — the decoder no longer blocks
    // on raw catchup completing.
    //
    // Safety vs the old deadlock (see prior comment at this location): the
    // transformation engine is still gated on `decode_catchup_done_tx` in
    // this commit, so catchup_decode_eth_calls must still pass `None` for
    // transform_tx to avoid filling the engine's buffered channel. A later
    // commit lifts the engine barrier and will flip this to `Some(...)`.
    // =========================================================================
    let configs = EthCallDecodeConfigs {
        regular: &regular_configs,
        once: &once_configs,
        event: &event_configs,
    };

    let catchup_fut = async {
        if skip_catchup || !raw_calls_dir.exists() {
            if !raw_calls_dir.exists() {
                tracing::info!(
                    "Eth_call decoding catchup complete for chain {} (no raw files)",
                    chain.name
                );
            }
            return Ok::<(), EthCallDecodingError>(());
        }

        let decode_start = Instant::now();
        catchup::catchup_decode_eth_calls(
            &raw_calls_dir,
            &output_base,
            &regular_configs,
            &once_configs,
            &event_configs,
            raw_data_config,
            None,
            repair,
            repair_scope.clone(),
        )
        .await?;

        tracing::info!(
            "Eth_call decoding catchup complete for chain {} in {:.1}s",
            chain.name,
            decode_start.elapsed().as_secs_f64()
        );

        Ok(())
    };

    let live_fut = current::decode_eth_calls_live(
        decoder_rx,
        &raw_calls_dir,
        &output_base,
        &chain.name,
        &configs,
        raw_data_config,
        &outputs,
        repair,
    );

    let (catchup_res, live_res) = tokio::join!(catchup_fut, live_fut);
    catchup_res?;
    live_res?;

    tracing::info!("Eth_call decoding complete for chain {}", chain.name);
    Ok(())
}
