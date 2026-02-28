use std::collections::{HashMap, HashSet};

use tokio::sync::mpsc::{Receiver, Sender};

use crate::decoding::DecoderMessage;
use crate::raw_data::historical::factories::FactoryAddressData;
use crate::raw_data::historical::logs::{process_completed_range, LogCollectionError, LogsCatchupState};
use crate::raw_data::historical::receipts::{LogData, LogMessage};
use crate::types::config::chain::ChainConfig;

pub async fn collect_logs(
    chain: &ChainConfig,
    mut log_rx: Receiver<LogMessage>,
    mut factory_rx: Option<Receiver<FactoryAddressData>>,
    decoder_tx: Option<Sender<DecoderMessage>>,
    mut state: LogsCatchupState,
) -> Result<(), LogCollectionError> {
    let has_factory_rx = factory_rx.is_some();
    state.needs_factory_wait = has_factory_rx && state.contract_logs_only;

    let mut range_data: HashMap<u64, Vec<LogData>> = HashMap::new();
    let mut range_factory_addresses: HashMap<u64, HashSet<[u8; 20]>> = HashMap::new();
    let mut pending_ranges: HashMap<u64, u64> = HashMap::new();
    let mut factory_ready: HashSet<u64> = HashSet::new();

    tracing::info!(
        "Starting log collection for chain {} (contract_logs_only: {}, waiting for factories: {})",
        chain.name,
        state.contract_logs_only,
        state.needs_factory_wait
    );

    loop {
        tokio::select! {
            log_result = log_rx.recv() => {
                match log_result {
                    Some(message) => {
                        match message {
                            LogMessage::Logs(logs) => {
                                for log in logs {
                                    let range_start = (log.block_number / state.range_size) * state.range_size;
                                    range_data.entry(range_start).or_default().push(log);
                                }
                            }
                            LogMessage::RangeComplete { range_start, range_end } => {
                                let factory_data_ready = !state.needs_factory_wait || factory_ready.contains(&range_start);

                                if factory_data_ready {
                                    process_completed_range(
                                        range_start,
                                        range_end,
                                        &mut range_data,
                                        &mut range_factory_addresses,
                                        state.contract_logs_only,
                                        &state.configured_addresses,
                                        &state.log_fields,
                                        &state.schema,
                                        &state.output_dir,
                                        &state.existing_files,
                                        &decoder_tx,
                                    )
                                    .await?;
                                    factory_ready.remove(&range_start);
                                } else {
                                    pending_ranges.insert(range_start, range_end);
                                }
                            }
                            LogMessage::AllRangesComplete => {
                                if pending_ranges.is_empty() {
                                    break;
                                }
                            }
                        }
                    }
                    None => {
                        tracing::warn!("log_rx channel closed unexpectedly");
                        break;
                    }
                }
            }

            factory_result = async {
                match &mut factory_rx {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                match factory_result {
                    Some(factory_data) => {
                        let range_start = factory_data.range_start;

                        let factory_addrs: HashSet<[u8; 20]> = factory_data
                            .addresses_by_block
                            .values()
                            .flatten()
                            .map(|(_, addr, _)| addr.0.0)
                            .collect();

                        tracing::debug!(
                            "Received {} factory addresses for range {}",
                            factory_addrs.len(),
                            range_start
                        );

                        range_factory_addresses
                            .entry(range_start)
                            .or_default()
                            .extend(factory_addrs);

                        if let Some(pending_end) = pending_ranges.remove(&range_start) {
                            process_completed_range(
                                range_start,
                                pending_end,
                                &mut range_data,
                                &mut range_factory_addresses,
                                state.contract_logs_only,
                                &state.configured_addresses,
                                &state.log_fields,
                                &state.schema,
                                &state.output_dir,
                                &state.existing_files,
                                &decoder_tx,
                            )
                            .await?;
                        } else {
                            factory_ready.insert(range_start);
                        }
                    }
                    None => {
                        tracing::debug!("factory_rx channel closed, pending_ranges: {}", pending_ranges.len());
                        factory_rx = None;
                    }
                }
            }
        }
    }

    if let Some(tx) = decoder_tx {
        let _ = tx.send(DecoderMessage::AllComplete).await;
    }

    tracing::info!("Log collection complete for chain {}", chain.name);
    Ok(())
}
