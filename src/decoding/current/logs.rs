//! Live/current phase for log decoding - processes new data as it arrives via channel.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use tokio::sync::mpsc::Receiver;

use crate::decoding::logs::{
    delete_decoded_logs_for_blocks, process_logs, process_logs_live, LogDecodingError,
};
use crate::decoding::types::{DecoderMessage, LogDecoderOutputs, LogMatcherConfig};
use crate::live::LiveStorage;
use crate::storage::contract_index::build_expected_factory_contracts_for_range;
use crate::storage::factory_data::load_accumulated_factory_addresses_from_parquet;
use crate::types::config::contract::Contracts;

/// Load accumulated factory addresses from both compacted parquet and uncompacted bincode files.
///
/// This loads all known factory addresses discovered so far, so they can be used
/// for decoding logs from any block, not just the block where they were discovered.
fn load_accumulated_factory_addresses(
    chain_name: &str,
) -> Result<HashMap<String, HashSet<[u8; 20]>>, LogDecodingError> {
    let mut result = load_accumulated_factory_addresses_from_parquet(chain_name)?;

    // 2. Load from uncompacted bincode files: data/{chain}/live/factories/{block}.bin
    let live_storage = LiveStorage::new(chain_name);
    if let Ok(factory_blocks) = live_storage.list_factory_blocks() {
        for block_number in factory_blocks {
            if let Ok(live_factories) = live_storage.read_factories(block_number) {
                for (collection_name, addresses) in live_factories.addresses_by_collection {
                    let addrs: HashSet<[u8; 20]> =
                        addresses.into_iter().map(|(_, addr)| addr).collect();
                    result.entry(collection_name).or_default().extend(addrs);
                }
            }
        }
    }

    let total_addrs: usize = result.values().map(|s| s.len()).sum();
    if total_addrs > 0 {
        tracing::info!(
            "Loaded {} accumulated factory addresses across {} collections",
            total_addrs,
            result.len()
        );
    }

    Ok(result)
}

/// Live phase: Process new log data as it arrives via channel.
/// Returns when AllComplete message is received or channel closes.
pub async fn decode_logs_live(
    decoder_rx: &mut Receiver<DecoderMessage>,
    matchers: &LogMatcherConfig<'_>,
    output_base: &Path,
    chain_name: &str,
    outputs: &LogDecoderOutputs<'_>,
    contracts: Option<&Contracts>,
) -> Result<(), LogDecodingError> {
    // Load accumulated factory addresses from parquet and bincode files
    let mut accumulated_factory_addresses = load_accumulated_factory_addresses(chain_name)?;

    // Live storage for live_mode=true messages
    let live_storage = LiveStorage::new(chain_name);

    tracing::info!("Log decoder live phase started, waiting for messages");

    loop {
        match decoder_rx.recv().await {
            Some(DecoderMessage::LogsReady {
                range_start,
                range_end,
                logs,
                live_mode,
                has_factory_matchers: _,
            }) => {
                tracing::debug!(
                    "Log decoder received {} logs for block {} (live_mode={})",
                    logs.len(),
                    range_start,
                    live_mode
                );
                if live_mode {
                    // Live mode: write to bincode storage
                    // For live mode, range_start == range_end (single block)
                    // Use the full accumulated factory addresses
                    process_logs_live(
                        logs.as_slice(),
                        range_start,
                        matchers,
                        &accumulated_factory_addresses,
                        &live_storage,
                        outputs,
                    )
                    .await?;
                } else {
                    // Historical mode: write to parquet
                    // Build per-range expected contracts so the contract index gets an entry
                    let expected_for_range =
                        contracts.map(|c| build_expected_factory_contracts_for_range(c, range_end));
                    process_logs(
                        logs.as_slice(),
                        range_start,
                        range_end,
                        matchers,
                        &accumulated_factory_addresses,
                        output_base,
                        outputs,
                        expected_for_range.as_ref(),
                    )
                    .await?;
                }
            }
            Some(DecoderMessage::FactoryAddresses { addresses, .. }) => {
                // Accumulate factory addresses - add new addresses to the accumulated set
                for (collection_name, addrs) in addresses {
                    let addr_set: HashSet<[u8; 20]> = addrs.iter().map(|a| a.0 .0).collect();
                    let count = addr_set.len();
                    accumulated_factory_addresses
                        .entry(collection_name.clone())
                        .or_default()
                        .extend(addr_set);
                    tracing::debug!(
                        "Added {} factory addresses for collection '{}', total now: {}",
                        count,
                        collection_name,
                        accumulated_factory_addresses
                            .get(&collection_name)
                            .map(|s| s.len())
                            .unwrap_or(0)
                    );
                }
            }
            Some(DecoderMessage::Reorg { orphaned, .. }) => {
                // Delete decoded data for orphaned blocks
                // Note: we do NOT remove factory addresses on reorg because:
                // - Once discovered, an address remains a valid contract
                // - The canonical chain will eventually include creation events for valid pools
                // - False positives are harmless (decoding events that don't exist is a no-op)
                tracing::info!(
                    "Handling reorg in log decoder, deleting {} orphaned blocks",
                    orphaned.len()
                );
                delete_decoded_logs_for_blocks(&live_storage, &orphaned)?;
            }
            Some(DecoderMessage::AllComplete) => {
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
