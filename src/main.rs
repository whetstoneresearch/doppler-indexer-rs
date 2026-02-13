#[cfg(feature = "bench")]
mod bench;
mod db;
mod raw_data;
mod rpc;
mod transformations;
mod types;

use std::env;
use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tracing_subscriber::EnvFilter;

use db::DbPool;
use raw_data::decoding::{decode_eth_calls, decode_logs, DecoderMessage};
use raw_data::historical::blocks::collect_blocks;
use raw_data::historical::eth_calls::collect_eth_calls;
use raw_data::historical::factories::{collect_factories, RecollectRequest};
use raw_data::historical::logs::collect_logs;
use raw_data::historical::receipts::{
    build_event_trigger_matchers, collect_receipts, EventTriggerMessage, LogMessage,
};
use rpc::UnifiedRpcClient;
use transformations::{
    build_registry, DecodedCallsMessage, DecodedEventsMessage, ExecutionMode,
    RangeCompleteMessage, TransformationEngine,
};
use types::config::chain::ChainConfig;
use types::config::eth_call::Frequency;
use types::config::indexer::IndexerConfig;

fn has_items<T>(opt: &Option<Vec<T>>) -> bool {
    opt.as_ref().is_some_and(|v| !v.is_empty())
}

fn optional_channel<T>(
    enabled: bool,
    capacity: usize,
) -> (Option<mpsc::Sender<T>>, Option<mpsc::Receiver<T>>) {
    if enabled {
        let (tx, rx) = mpsc::channel(capacity);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = IndexerConfig::load(Path::new("config/config.json"))?;
    load_required_env_vars(&config)?;

    #[cfg(feature = "bench")]
    {
        std::fs::create_dir_all("data")?;
        bench::init(Path::new("data/bench.csv"))?;
        tracing::info!("Benchmarking enabled, writing to data/bench.csv");
    }

    tracing::info!("Loaded config with {} chain(s)", config.chains.len());

    for chain in &config.chains {
        process_chain(&config, chain).await?;
    }

    tracing::info!("All chains processed successfully");
    Ok(())
}

/// Ensures all required RPC URL env vars are set, loading .env if needed.
fn load_required_env_vars(config: &IndexerConfig) -> anyhow::Result<()> {
    let required: Vec<&str> = config
        .chains
        .iter()
        .map(|c| c.rpc_url_env_var.as_str())
        .collect();

    let missing: Vec<&&str> = required
        .iter()
        .filter(|var| env::var(var).is_err())
        .collect();

    if missing.is_empty() {
        return Ok(());
    }

    dotenvy::dotenv().with_context(|| {
        format!(
            "Missing env vars {:?} and failed to load .env file",
            missing
        )
    })?;

    let still_missing: Vec<&str> = required
        .iter()
        .filter(|var| env::var(var).is_err())
        .copied()
        .collect();

    anyhow::ensure!(
        still_missing.is_empty(),
        "Missing required env vars after loading .env: {:?}",
        still_missing
    );

    Ok(())
}

async fn process_chain(config: &IndexerConfig, chain: &ChainConfig) -> anyhow::Result<()> {
    tracing::info!("Processing chain: {}", chain.name);

    let rpc_url = env::var(&chain.rpc_url_env_var).with_context(|| {
        format!(
            "env var {} not set for chain {}",
            chain.rpc_url_env_var, chain.name
        )
    })?;

    // Feature detection
    let has_factories = chain
        .contracts
        .values()
        .any(|c| has_items(&c.factories));

    let contract_logs_only = config
        .raw_data_collection
        .contract_logs_only
        .unwrap_or(false);

    let needs_factory_filtering = has_factories && contract_logs_only;

    let has_factory_calls = chain.contracts.values().any(|c| {
        c.factories.as_ref().is_some_and(|factories| {
            factories.iter().any(|f| has_items(&f.calls))
        })
    });

    let has_event_triggered_calls = chain.contracts.values().any(|c| {
        c.calls.as_ref().is_some_and(|calls| {
            calls
                .iter()
                .any(|call| matches!(call.frequency, Frequency::OnEvents(_)))
        }) || c.factories.as_ref().is_some_and(|factories| {
            factories.iter().any(|f| {
                f.calls.as_ref().is_some_and(|calls| {
                    calls
                        .iter()
                        .any(|call| matches!(call.frequency, Frequency::OnEvents(_)))
                })
            })
        })
    });

    let has_events = chain.contracts.values().any(|c| {
        has_items(&c.events)
            || c.factories
                .as_ref()
                .is_some_and(|f| f.iter().any(|fc| has_items(&fc.events)))
    });

    let has_calls = chain.contracts.values().any(|c| {
        has_items(&c.calls)
            || c.factories
                .as_ref()
                .is_some_and(|f| f.iter().any(|fc| has_items(&fc.calls)))
    });

    tracing::info!(
        "Chain {} - has_factories: {}, contract_logs_only: {}, has_factory_calls: {}, has_event_triggered_calls: {}",
        chain.name, has_factories, contract_logs_only, has_factory_calls, has_event_triggered_calls
    );
    tracing::info!(
        "Chain {} - decode_logs: {}, decode_calls: {}",
        chain.name, has_events, has_calls
    );

    // Channel setup
    let channel_cap = config.raw_data_collection.channel_capacity.unwrap_or(1000);
    let factory_cap = config
        .raw_data_collection
        .factory_channel_capacity
        .unwrap_or(1000);

    let (block_tx, block_rx) = mpsc::channel(channel_cap);
    let (log_tx, log_rx) = mpsc::channel::<LogMessage>(channel_cap);
    let (eth_call_tx, eth_call_rx) = mpsc::channel(channel_cap);

    let (factory_log_tx, factory_log_rx) =
        optional_channel::<LogMessage>(has_factories, channel_cap);
    let (logs_factory_tx, logs_factory_rx) =
        optional_channel(needs_factory_filtering, factory_cap);
    let (eth_calls_factory_tx, eth_calls_factory_rx) =
        optional_channel(has_factory_calls, factory_cap);
    let (event_trigger_tx, event_trigger_rx) =
        optional_channel::<EventTriggerMessage>(has_event_triggered_calls, channel_cap);
    let (log_decoder_tx, log_decoder_rx) =
        optional_channel::<DecoderMessage>(has_events, channel_cap);
    let (call_decoder_tx, call_decoder_rx) =
        optional_channel::<DecoderMessage>(has_calls, channel_cap);
    let (recollect_tx, recollect_rx) =
        optional_channel::<RecollectRequest>(has_factories, channel_cap);

    let event_matchers = if has_event_triggered_calls {
        build_event_trigger_matchers(&chain.contracts)
    } else {
        Vec::new()
    };

    // Transformation setup
    let registry = build_registry();
    let transformations_enabled = config.transformations.is_some() && !registry.is_empty();

    let (transform_events_tx, transform_events_rx) =
        optional_channel::<DecodedEventsMessage>(transformations_enabled, channel_cap);
    let (transform_calls_tx, transform_calls_rx) =
        optional_channel::<DecodedCallsMessage>(transformations_enabled, channel_cap);
    let (_transform_complete_tx, transform_complete_rx) =
        optional_channel::<RangeCompleteMessage>(transformations_enabled, channel_cap);

    let db_pool = if transformations_enabled {
        let tc = config.transformations.as_ref().unwrap();
        let database_url = env::var(&tc.database_url_env_var).with_context(|| {
            format!(
                "env var {} not set for transformations",
                tc.database_url_env_var
            )
        })?;

        let pool = DbPool::new(&database_url)
            .await
            .context("failed to create database pool")?;
        pool.run_migrations()
            .await
            .context("failed to run database migrations")?;

        tracing::info!("Database pool initialized and migrations complete");
        Some(Arc::new(pool))
    } else {
        None
    };

    // Shared state for spawned tasks
    let chain = Arc::new(chain.clone());
    let raw_config = Arc::new(config.raw_data_collection.clone());

    // Pre-create RPC clients to avoid unwrap() inside spawned tasks
    let blocks_client = UnifiedRpcClient::from_url(&rpc_url)?;
    let receipts_client = UnifiedRpcClient::from_url(&rpc_url)?;
    let eth_calls_client = UnifiedRpcClient::from_url(&rpc_url)?;

    // Clone decoder senders for factories before the originals are moved
    let log_decoder_tx_for_factories = if has_factories {
        log_decoder_tx.clone()
    } else {
        None
    };
    let call_decoder_tx_for_factories = if has_factories {
        call_decoder_tx.clone()
    } else {
        None
    };

    let mut tasks: JoinSet<anyhow::Result<()>> = JoinSet::new();

    tasks.spawn({
        let (chain, cfg) = (chain.clone(), raw_config.clone());
        async move {
            collect_blocks(&chain, &blocks_client, &cfg, Some(block_tx), Some(eth_call_tx))
                .await
                .context("block collection failed")
        }
    });

    tasks.spawn({
        let (chain, cfg) = (chain.clone(), raw_config.clone());
        async move {
            collect_receipts(
                &chain,
                &receipts_client,
                &cfg,
                block_rx,
                Some(log_tx),
                factory_log_tx,
                event_trigger_tx,
                event_matchers,
                recollect_rx,
            )
            .await
            .context("receipt collection failed")
        }
    });

    tasks.spawn({
        let (chain, cfg) = (chain.clone(), raw_config.clone());
        async move {
            collect_logs(&chain, &cfg, log_rx, logs_factory_rx, log_decoder_tx)
                .await
                .context("log collection failed")
        }
    });

    tasks.spawn({
        let (chain, cfg) = (chain.clone(), raw_config.clone());
        async move {
            collect_eth_calls(
                &chain,
                &eth_calls_client,
                &cfg,
                eth_call_rx,
                eth_calls_factory_rx,
                event_trigger_rx,
                call_decoder_tx,
            )
            .await
            .context("eth call collection failed")
        }
    });

    if has_factories {
        tasks.spawn({
            let (chain, cfg) = (chain.clone(), raw_config.clone());
            async move {
                collect_factories(
                    &chain,
                    &cfg,
                    factory_log_rx.unwrap(),
                    logs_factory_tx,
                    eth_calls_factory_tx,
                    log_decoder_tx_for_factories,
                    call_decoder_tx_for_factories,
                    recollect_tx,
                )
                .await
                .context("factory collection failed")
            }
        });
    }

    if has_events {
        let transform_events_tx = transform_events_tx.clone();
        tasks.spawn({
            let (chain, cfg) = (chain.clone(), raw_config.clone());
            async move {
                decode_logs(&chain, &cfg, log_decoder_rx.unwrap(), transform_events_tx)
                    .await
                    .context("log decoding failed")
            }
        });
    }

    if has_calls {
        let transform_calls_tx = transform_calls_tx.clone();
        tasks.spawn({
            let (chain, cfg) = (chain.clone(), raw_config.clone());
            async move {
                decode_eth_calls(&chain, &cfg, call_decoder_rx.unwrap(), transform_calls_tx)
                    .await
                    .context("eth call decoding failed")
            }
        });
    }

    if transformations_enabled {
        let rpc_client = Arc::new(UnifiedRpcClient::from_url(&rpc_url)?);
        let tc = config.transformations.as_ref().unwrap();
        let mode = if tc.mode.batch_for_catchup {
            ExecutionMode::Batch {
                batch_size: tc.mode.catchup_batch_size,
            }
        } else {
            ExecutionMode::Streaming
        };

        let handler_count = registry.handler_count();
        let engine = TransformationEngine::new(
            Arc::new(registry),
            db_pool.unwrap(),
            rpc_client,
            chain.name.clone(),
            chain.chain_id,
            mode,
        )
        .await
        .context("failed to create transformation engine")?;

        engine
            .initialize()
            .await
            .context("failed to initialize transformation handlers")?;

        tracing::info!(
            "Transformation engine initialized for chain {} with {} handlers",
            chain.name,
            handler_count
        );

        tasks.spawn(async move {
            engine
                .run(
                    transform_events_rx.unwrap(),
                    transform_calls_rx.unwrap(),
                    transform_complete_rx.unwrap(),
                )
                .await
                .map_err(|e| anyhow::anyhow!("transformation engine error: {}", e))
        });
    }

    while let Some(result) = tasks.join_next().await {
        result.context("pipeline task panicked")??;
    }

    tracing::info!("Completed collection for chain {}", chain.name);
    Ok(())
}
