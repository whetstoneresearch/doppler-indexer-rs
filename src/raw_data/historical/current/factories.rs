use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use alloy::primitives::Address;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::decoding::DecoderMessage;
use crate::raw_data::historical::factories::{
    process_range, FactoryAddressData, FactoryCollectionError, FactoryMatcher, FactoryMessage,
};
use crate::raw_data::historical::receipts::{LogData, LogMessage};
use crate::types::config::chain::ChainConfig;
use crate::types::config::raw_data::RawDataCollectionConfig;

pub async fn collect_factories(
    chain: &ChainConfig,
    raw_data_config: &RawDataCollectionConfig,
    mut log_rx: Receiver<LogMessage>,
    logs_factory_tx: Option<Sender<FactoryAddressData>>,
    eth_calls_factory_tx: Option<Sender<FactoryMessage>>,
    log_decoder_tx: Option<Sender<DecoderMessage>>,
    call_decoder_tx: Option<Sender<DecoderMessage>>,
    matchers: Arc<Vec<FactoryMatcher>>,
    existing_files: Arc<HashSet<String>>,
    output_dir: Arc<PathBuf>,
) -> Result<(), FactoryCollectionError> {
    let range_size = raw_data_config.parquet_block_range.unwrap_or(1000) as u64;

    // If no matchers, forward empty ranges from channel
    if matchers.is_empty() {
        while let Some(message) = log_rx.recv().await {
            match message {
                LogMessage::Logs(_) => {}
                LogMessage::RangeComplete {
                    range_start,
                    range_end,
                } => {
                    let empty_data = FactoryAddressData {
                        range_start,
                        range_end,
                        addresses_by_block: HashMap::new(),
                    };

                    if let Some(ref tx) = logs_factory_tx {
                        if tx.send(empty_data.clone()).await.is_err() {
                            tracing::error!(
                                "Failed to send empty factory data for range {}-{} to logs_factory_tx - receiver dropped",
                                range_start,
                                range_end
                            );
                            return Err(FactoryCollectionError::ChannelSend(format!(
                                "logs_factory_tx (empty data {}-{}) - receiver dropped",
                                range_start, range_end
                            )));
                        }
                    }

                    if let Some(ref tx) = eth_calls_factory_tx {
                        let _ = tx
                            .send(FactoryMessage::RangeComplete {
                                range_start,
                                range_end,
                            })
                            .await;
                    }

                    // Send empty addresses to decoders
                    if let Some(ref tx) = log_decoder_tx {
                        let _ = tx
                            .send(DecoderMessage::FactoryAddresses {
                                range_start,
                                range_end,
                                addresses: HashMap::new(),
                            })
                            .await;
                    }
                    if let Some(ref tx) = call_decoder_tx {
                        let _ = tx
                            .send(DecoderMessage::FactoryAddresses {
                                range_start,
                                range_end,
                                addresses: HashMap::new(),
                            })
                            .await;
                    }
                }
                LogMessage::AllRangesComplete => {
                    // Signal all complete to eth_calls
                    if let Some(ref tx) = eth_calls_factory_tx {
                        let _ = tx.send(FactoryMessage::AllComplete).await;
                    }
                    break;
                }
            }
        }

        return Ok(());
    }

    // =========================================================================
    // Streaming phase: Process new logs from channel
    // =========================================================================
    let mut range_data: HashMap<u64, Vec<LogData>> = HashMap::new();

    tracing::info!(
        "Starting factory collection for chain {} with {} matchers",
        chain.name,
        matchers.len()
    );

    loop {
        let message = match log_rx.recv().await {
            Some(msg) => msg,
            None => break,
        };

        match message {
            LogMessage::Logs(logs) => {
                for log in logs {
                    let range_start = (log.block_number / range_size) * range_size;
                    range_data.entry(range_start).or_default().push(log);
                }
            }
            LogMessage::RangeComplete {
                range_start,
                range_end,
            } => {
                let logs = range_data.remove(&range_start).unwrap_or_default();

                let factory_data = match process_range(
                    range_start,
                    range_end,
                    logs,
                    &matchers,
                    &output_dir,
                    &existing_files,
                )
                .await
                {
                    Ok(data) => data,
                    Err(e) => {
                        tracing::error!(
                            "Factory processing failed for range {}-{}: {:?}",
                            range_start,
                            range_end,
                            e
                        );
                        return Err(e);
                    }
                };

                if let Some(ref tx) = logs_factory_tx {
                    if tx.send(factory_data.clone()).await.is_err() {
                        tracing::error!(
                            "Failed to send factory data for range {}-{} to logs_factory_tx - receiver dropped",
                            factory_data.range_start,
                            factory_data.range_end
                        );
                        return Err(FactoryCollectionError::ChannelSend(format!(
                            "logs_factory_tx ({}-{}) - receiver dropped",
                            factory_data.range_start, factory_data.range_end
                        )));
                    }
                }

                if let Some(ref tx) = eth_calls_factory_tx {
                    // Send incremental addresses then range complete
                    let _ = tx
                        .send(FactoryMessage::IncrementalAddresses(factory_data.clone()))
                        .await;
                    let _ = tx
                        .send(FactoryMessage::RangeComplete {
                            range_start: factory_data.range_start,
                            range_end: factory_data.range_end,
                        })
                        .await;
                }

                // Send to decoders
                let addresses: HashMap<String, Vec<Address>> = factory_data
                    .addresses_by_block
                    .values()
                    .flatten()
                    .fold(HashMap::new(), |mut acc, (_, addr, collection)| {
                        acc.entry(collection.clone()).or_default().push(*addr);
                        acc
                    });

                if let Some(ref tx) = log_decoder_tx {
                    let _ = tx
                        .send(DecoderMessage::FactoryAddresses {
                            range_start: factory_data.range_start,
                            range_end: factory_data.range_end,
                            addresses: addresses.clone(),
                        })
                        .await;
                }

                if let Some(ref tx) = call_decoder_tx {
                    let _ = tx
                        .send(DecoderMessage::FactoryAddresses {
                            range_start: factory_data.range_start,
                            range_end: factory_data.range_end,
                            addresses,
                        })
                        .await;
                }
            }
            LogMessage::AllRangesComplete => {
                // Signal all complete to eth_calls
                if let Some(ref tx) = eth_calls_factory_tx {
                    let _ = tx.send(FactoryMessage::AllComplete).await;
                }
                break;
            }
        }
    }

    tracing::info!("Factory collection complete for chain {}", chain.name);
    Ok(())
}
