#[cfg(feature = "bench")]
mod bench;
mod raw_data;
mod rpc;
mod types;

use std::env;
use std::path::Path;

use tokio::sync::mpsc;
use tracing_subscriber::EnvFilter;

use raw_data::decoding::{decode_eth_calls, decode_logs, DecoderMessage};
use raw_data::historical::blocks::collect_blocks;
use raw_data::historical::eth_calls::collect_eth_calls;
use raw_data::historical::factories::collect_factories;
use raw_data::historical::logs::collect_logs;
use raw_data::historical::receipts::{
    build_event_trigger_matchers, collect_receipts, EventTriggerMessage, LogMessage,
};
use rpc::UnifiedRpcClient;
use types::config::eth_call::Frequency;
use types::config::indexer::IndexerConfig;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = IndexerConfig::load(Path::new("config/config.json"))?;

    #[cfg(feature = "bench")]
    {
        std::fs::create_dir_all("data")?;
        bench::init(Path::new("data/bench.csv"))?;
        tracing::info!("Benchmarking enabled, writing to data/bench.csv");
    }

    tracing::info!("Loaded config with {} chain(s)", config.chains.len());

    for chain in &config.chains {
        tracing::info!("Processing chain: {}", chain.name);

        let rpc_url = env::var(&chain.rpc_url_env_var).map_err(|_| {
            anyhow::anyhow!(
                "Environment variable {} not set for chain {}",
                chain.rpc_url_env_var,
                chain.name
            )
        })?;

        let client = UnifiedRpcClient::from_url(&rpc_url)?;
        tracing::info!("Connected to RPC for chain {}", chain.name);

        let has_factories = chain
            .contracts
            .values()
            .any(|c| c.factories.as_ref().map(|f| !f.is_empty()).unwrap_or(false));

        let contract_logs_only = config
            .raw_data_collection
            .contract_logs_only
            .unwrap_or(false);
        let needs_factory_filtering = has_factories && contract_logs_only;

        let has_factory_calls = chain.contracts.values().any(|c| {
            c.factories
                .as_ref()
                .map(|factories| {
                    factories
                        .iter()
                        .any(|f| f.calls.as_ref().map(|c| !c.is_empty()).unwrap_or(false))
                })
                .unwrap_or(false)
        });

        // Check if any calls use on_events frequency
        let has_event_triggered_calls = chain.contracts.values().any(|c| {
            c.calls
                .as_ref()
                .map(|calls| {
                    calls
                        .iter()
                        .any(|call| matches!(call.frequency, Frequency::OnEvents(_)))
                })
                .unwrap_or(false)
                || c.factories
                    .as_ref()
                    .map(|factories| {
                        factories.iter().any(|f| {
                            f.calls
                                .as_ref()
                                .map(|calls| {
                                    calls
                                        .iter()
                                        .any(|call| matches!(call.frequency, Frequency::OnEvents(_)))
                                })
                                .unwrap_or(false)
                        })
                    })
                    .unwrap_or(false)
        });

        tracing::info!(
            "Chain {} - has_factories: {}, contract_logs_only: {}, has_factory_calls: {}, has_event_triggered_calls: {}",
            chain.name,
            has_factories,
            contract_logs_only,
            has_factory_calls,
            has_event_triggered_calls
        );

        // Channel capacities from config, with sensible defaults
        let channel_capacity = config.raw_data_collection.channel_capacity.unwrap_or(1000);
        let factory_channel_capacity = config
            .raw_data_collection
            .factory_channel_capacity
            .unwrap_or(1000);

        let (block_tx, block_rx) = mpsc::channel(channel_capacity);
        let (log_tx, log_rx) = mpsc::channel::<LogMessage>(channel_capacity);
        let (eth_call_tx, eth_call_rx) = mpsc::channel(channel_capacity);

        let (factory_log_tx, factory_log_rx) = if has_factories {
            let (tx, rx) = mpsc::channel::<LogMessage>(channel_capacity);
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        let (logs_factory_tx, logs_factory_rx) = if needs_factory_filtering {
            let (tx, rx) = mpsc::channel(factory_channel_capacity);
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        let (eth_calls_factory_tx, eth_calls_factory_rx) = if has_factory_calls {
            let (tx, rx) = mpsc::channel(factory_channel_capacity);
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        // Create event trigger channel for on_events frequency calls
        let (event_trigger_tx, event_trigger_rx) = if has_event_triggered_calls {
            let (tx, rx) = mpsc::channel::<EventTriggerMessage>(channel_capacity);
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        // Build event trigger matchers from contracts config
        let event_matchers = if has_event_triggered_calls {
            build_event_trigger_matchers(&chain.contracts)
        } else {
            Vec::new()
        };

        // Check if decoding is needed
        let has_events = chain.contracts.values().any(|c| {
            c.events.as_ref().map(|e| !e.is_empty()).unwrap_or(false)
                || c.factories
                    .as_ref()
                    .map(|f| {
                        f.iter()
                            .any(|fc| fc.events.as_ref().map(|e| !e.is_empty()).unwrap_or(false))
                    })
                    .unwrap_or(false)
        });

        let has_calls = chain.contracts.values().any(|c| {
            c.calls
                .as_ref()
                .map(|calls| !calls.is_empty())
                .unwrap_or(false)
                || c.factories.as_ref().map(|f| {
                    f.iter().any(|fc| {
                        fc.calls
                            .as_ref()
                            .map(|calls| !calls.is_empty())
                            .unwrap_or(false)
                    })
                }).unwrap_or(false)
        });

        let decode_logs_enabled = has_events;
        let decode_calls_enabled = has_calls;

        tracing::info!(
            "Chain {} - decode_logs: {}, decode_calls: {}",
            chain.name,
            decode_logs_enabled,
            decode_calls_enabled
        );

        // Create decoder channels
        let (log_decoder_tx, log_decoder_rx) = if decode_logs_enabled {
            let (tx, rx) = mpsc::channel::<DecoderMessage>(channel_capacity);
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        let (call_decoder_tx, call_decoder_rx) = if decode_calls_enabled {
            let (tx, rx) = mpsc::channel::<DecoderMessage>(channel_capacity);
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        let raw_data_config = config.raw_data_collection.clone();
        let raw_data_config2 = config.raw_data_collection.clone();
        let raw_data_config3 = config.raw_data_collection.clone();
        let raw_data_config4 = config.raw_data_collection.clone();
        let raw_data_config5 = config.raw_data_collection.clone();
        let raw_data_config6 = config.raw_data_collection.clone();
        let raw_data_config7 = config.raw_data_collection.clone();
        let chain_clone = chain.clone();
        let chain_clone2 = chain.clone();
        let chain_clone3 = chain.clone();
        let chain_clone4 = chain.clone();
        let chain_clone5 = chain.clone();
        let chain_clone6 = chain.clone();
        let chain_clone7 = chain.clone();

        // Clone decoder senders for factories (needs both)
        let log_decoder_tx_for_factories = log_decoder_tx.clone();
        let call_decoder_tx_for_factories = call_decoder_tx.clone();
        
        let blocks_handle = tokio::spawn(async move {
            collect_blocks(
                &chain_clone,
                &client,
                &raw_data_config,
                Some(block_tx),
                Some(eth_call_tx),
            )
            .await
        });

        let receipts_handle = tokio::spawn(async move {
            let rpc_url = env::var(&chain_clone2.rpc_url_env_var).unwrap();
            let client = UnifiedRpcClient::from_url(&rpc_url).unwrap();
            collect_receipts(
                &chain_clone2,
                &client,
                &raw_data_config2,
                block_rx,
                Some(log_tx),
                factory_log_tx,
                event_trigger_tx,
                event_matchers,
            )
            .await
        });

        let logs_handle = tokio::spawn(async move {
            collect_logs(&chain_clone3, &raw_data_config3, log_rx, logs_factory_rx, log_decoder_tx).await
        });

        let eth_calls_handle = tokio::spawn(async move {
            let rpc_url = env::var(&chain_clone4.rpc_url_env_var).unwrap();
            let client = UnifiedRpcClient::from_url(&rpc_url).unwrap();
            collect_eth_calls(
                &chain_clone4,
                &client,
                &raw_data_config4,
                eth_call_rx,
                eth_calls_factory_rx,
                event_trigger_rx,
                call_decoder_tx,
            )
            .await
        });

        let factories_handle = if has_factories {
            Some(tokio::spawn(async move {
                collect_factories(
                    &chain_clone5,
                    &raw_data_config5,
                    factory_log_rx.unwrap(),
                    logs_factory_tx,
                    eth_calls_factory_tx,
                    log_decoder_tx_for_factories,
                    call_decoder_tx_for_factories,
                )
                .await
            }))
        } else {
            None
        };

        // Spawn decoder tasks
        let log_decoder_handle = if decode_logs_enabled {
            Some(tokio::spawn(async move {
                decode_logs(
                    &chain_clone6,
                    &raw_data_config6,
                    log_decoder_rx.unwrap(),
                )
                .await
            }))
        } else {
            None
        };

        let call_decoder_handle = if decode_calls_enabled {
            Some(tokio::spawn(async move {
                decode_eth_calls(
                    &chain_clone7,
                    &raw_data_config7,
                    call_decoder_rx.unwrap(),
                )
                .await
            }))
        } else {
            None
        };

        let (blocks_result, receipts_result, logs_result, eth_calls_result, factories_result, log_decoder_result, call_decoder_result) =
            tokio::try_join!(
                blocks_handle,
                receipts_handle,
                logs_handle,
                eth_calls_handle,
                async {
                    match factories_handle {
                        Some(handle) => handle.await,
                        None => Ok(Ok(())),
                    }
                },
                async {
                    match log_decoder_handle {
                        Some(handle) => handle.await,
                        None => Ok(Ok(())),
                    }
                },
                async {
                    match call_decoder_handle {
                        Some(handle) => handle.await,
                        None => Ok(Ok(())),
                    }
                }
            )?;

        blocks_result?;
        receipts_result?;
        logs_result?;
        eth_calls_result?;
        factories_result?;
        log_decoder_result?;
        call_decoder_result?;

        tracing::info!("Completed collection for chain {}", chain.name);
    }

    tracing::info!("All chains processed successfully");
    Ok(())
}
