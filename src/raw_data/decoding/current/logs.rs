//! Live/current phase for log decoding - processes new data as it arrives via channel.

use std::collections::{HashMap, HashSet};

use tokio::sync::mpsc::{Receiver, Sender};

use crate::raw_data::decoding::logs::{process_logs, EventMatcher, LogDecodingError};
use crate::raw_data::decoding::types::DecoderMessage;
use crate::transformations::{DecodedEventsMessage, RangeCompleteMessage};

/// Live phase: Process new log data as it arrives via channel.
/// Returns when AllComplete message is received or channel closes.
pub async fn decode_logs_live(
    decoder_rx: &mut Receiver<DecoderMessage>,
    regular_matchers: &[EventMatcher],
    factory_matchers: &HashMap<String, Vec<EventMatcher>>,
    output_base: &std::path::Path,
    transform_tx: Option<&Sender<DecodedEventsMessage>>,
    complete_tx: Option<&Sender<RangeCompleteMessage>>,
) -> Result<(), LogDecodingError> {
    // Track factory addresses per range
    let mut factory_addresses: HashMap<u64, HashMap<String, HashSet<[u8; 20]>>> = HashMap::new();

    loop {
        match decoder_rx.recv().await {
            Some(DecoderMessage::LogsReady {
                range_start,
                range_end,
                logs,
            }) => {
                // Get factory addresses for this range
                let factory_addrs = factory_addresses.remove(&range_start).unwrap_or_default();

                process_logs(
                    &logs,
                    range_start,
                    range_end,
                    regular_matchers,
                    factory_matchers,
                    &factory_addrs,
                    output_base,
                    transform_tx,
                    complete_tx,
                )
                .await?;
            }
            Some(DecoderMessage::FactoryAddresses {
                range_start,
                addresses,
                ..
            }) => {
                // Store factory addresses for when logs arrive
                let addrs_by_collection: HashMap<String, HashSet<[u8; 20]>> = addresses
                    .into_iter()
                    .map(|(name, addrs)| {
                        let set: HashSet<[u8; 20]> = addrs.iter().map(|a| a.0 .0).collect();
                        (name, set)
                    })
                    .collect();
                factory_addresses.insert(range_start, addrs_by_collection);
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
