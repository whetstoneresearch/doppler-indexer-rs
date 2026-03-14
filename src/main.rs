#[cfg(feature = "bench")]
mod bench;
mod db;
mod decoding;
mod live;
mod metrics;
mod raw_data;
mod rpc;
mod runtime;
mod storage;
mod transformations;
mod types;

use std::env;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinSet;
use tracing_subscriber::EnvFilter;

use db::DbPool;
use decoding::{decode_eth_calls, decode_logs, DecoderMessage};
use live::{
    CompactionService, LiveCollector, LiveEthCallCollector, LiveMessage, LiveModeConfig,
    LivePipelineExpectations, LiveProgressTracker, TransformRetryRequest,
};
use raw_data::historical::catchup::blocks::collect_blocks;
use raw_data::historical::factories::{build_factory_matchers, FactoryMessage, RecollectRequest};
use raw_data::historical::receipts::{
    build_event_trigger_matchers, EventTriggerMessage, LogMessage,
};
use rpc::{UnifiedRpcClient, WsClient};
use runtime::{build_rpc_client_with_limiter, ChainFeatures, ChainRuntime, CommonChannels};
use storage::{InitialSyncService, LocalBackend, RetryQueue, S3Backend, StorageManager};
use transformations::{
    build_registry, DecodedCallsMessage, DecodedEventsMessage, ExecutionMode,
    RangeCompleteMessage, ReorgMessage, TransformationEngine,
};
use types::config::chain::ChainConfig;
use types::config::defaults::{raw_data as raw_data_defaults, rpc as rpc_defaults};
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

fn live_pipeline_expectations(
    expect_log_decode: bool,
    expect_eth_call_collection: bool,
    expect_eth_call_decode: bool,
    expect_transformations: bool,
) -> LivePipelineExpectations {
    LivePipelineExpectations {
        expect_log_decode,
        expect_eth_call_collection,
        expect_eth_call_decode,
        expect_transformations,
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args: Vec<String> = env::args().collect();
    let decode_only = args.iter().any(|a| a == "--decode-only");
    let live_only = args.iter().any(|a| a == "--live-only");

    if live_only && decode_only {
        anyhow::bail!("Cannot use --live-only and --decode-only together");
    }

    let config = IndexerConfig::load(Path::new("config/config.json"))?;
    if !decode_only {
        load_required_env_vars(&config)?;
    }

    #[cfg(feature = "bench")]
    {
        std::fs::create_dir_all("data")?;
        bench::init(Path::new("data/bench.csv"))?;
        tracing::info!("Benchmarking enabled, writing to data/bench.csv");
    }

    // Initialize metrics server if configured
    if let Some(ref metrics_config) = config.metrics {
        let addr = metrics_config
            .addr
            .parse()
            .context("Invalid metrics.addr in config")?;
        metrics::init_metrics_server(addr);
        metrics::describe_rpc_metrics();
    }

    // Create retry queue before StorageManager if S3 is configured
    let retry_queue = if let Some(s3_config) = config.storage.as_ref().and_then(|s| s.s3.as_ref()) {
        let sync_config = config
            .storage
            .as_ref()
            .and_then(|s| s.sync.clone())
            .unwrap_or_default();
        let s3_backend = S3Backend::from_config(s3_config)
            .context("failed to create S3 backend for retry queue")?;
        let queue = Arc::new(RetryQueue::new(
            PathBuf::from("data"),
            sync_config,
            s3_backend,
        ));
        if let Err(e) = queue.load().await {
            tracing::warn!("Failed to load retry queue: {}", e);
        }
        Some(queue)
    } else {
        None
    };

    // Initialize storage manager with retry queue
    let storage_manager = StorageManager::new(
        config.storage.as_ref(),
        PathBuf::from("data"),
        retry_queue.clone(),
    )
    .await
    .context("failed to initialize storage manager")?;

    if storage_manager.is_s3_enabled() {
        // Register all chains with manifest manager before refresh
        for chain in &config.chains {
            storage_manager.register_chain(&chain.name);
        }

        tracing::info!("S3 storage enabled, refreshing manifest...");
        storage_manager
            .refresh_manifest()
            .await
            .context("failed to refresh S3 manifest")?;
    }

    let storage_manager = Arc::new(storage_manager);

    // Start periodic manifest refresh and other S3 services if enabled
    if storage_manager.is_s3_enabled() {
        // Periodic manifest refresh task
        let sm = storage_manager.clone();
        let refresh_secs = config
            .storage
            .as_ref()
            .and_then(|s| s.sync.as_ref())
            .map(|s| s.manifest_refresh_secs)
            .unwrap_or(60);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(refresh_secs));
            loop {
                interval.tick().await;
                if let Err(e) = sm.refresh_manifest().await {
                    tracing::warn!("Periodic manifest refresh failed: {}", e);
                } else {
                    tracing::debug!("Periodic manifest refresh completed");
                }
            }
        });

        tracing::info!(
            "Periodic manifest refresh enabled (every {}s)",
            refresh_secs
        );
    }

    // Start retry queue and initial sync services if S3 is enabled
    if storage_manager.is_s3_enabled() {
        // Spawn retry queue background task
        if let Some(queue) = retry_queue {
            let queue_handle = queue.clone();
            tokio::spawn(async move {
                let (_tx, rx) = tokio::sync::oneshot::channel::<()>();
                queue_handle.run(rx).await;
            });
            tracing::info!("Retry queue service started");
        }

        // Start initial sync service to upload existing local data to S3.
        // This ensures data collected before S3 was configured gets synced.
        if let Some(manifest_manager) = storage_manager.manifest_manager() {
            // Scan correct paths matching actual data layout
            let prefixes: Vec<String> = config
                .chains
                .iter()
                .flat_map(|chain| {
                    vec![
                        // Historical data: data/{chain}/historical/...
                        format!("{}/historical/raw", chain.name),
                        format!("{}/historical/decoded", chain.name),
                        format!("{}/historical/factories", chain.name),
                        // Live compacted data
                        format!("raw/{}", chain.name),
                        format!("derived/{}", chain.name),
                    ]
                })
                .collect();

            let initial_sync = Arc::new(InitialSyncService::with_manifest_manager(
                LocalBackend::new(storage_manager.local_base().clone()),
                manifest_manager.s3_backend().clone(),
                prefixes,
                manifest_manager,
            ));

            tokio::spawn(async move {
                let (_tx, rx) = tokio::sync::oneshot::channel::<()>();
                if let Err(e) = initial_sync.run(rx).await {
                    tracing::error!("Initial S3 sync failed: {}", e);
                }
            });

            tracing::info!("Initial S3 sync service started");
        }
    }

    if decode_only {
        tracing::info!("Running in decode-only mode (no collection, no transformations)");
    } else if live_only {
        tracing::info!("Running in live-only mode (skips historical processing)");
    }

    tracing::info!("Loaded config with {} chain(s)", config.chains.len());

    for chain in &config.chains {
        if live_only {
            process_chain_live_only(&config, chain).await?;
        } else if decode_only {
            decode_only_chain(&config, chain).await?;
        } else {
            process_chain(&config, chain, storage_manager.clone()).await?;
        }
    }

    tracing::info!("All chains processed successfully");
    Ok(())
}

/// Ensures all required RPC/WS URL env vars are set, loading .env if needed.
fn load_required_env_vars(config: &IndexerConfig) -> anyhow::Result<()> {
    let mut required: Vec<&str> = config
        .chains
        .iter()
        .map(|c| c.rpc_url_env_var.as_str())
        .collect();

    // Also include WS URL vars when configured
    for chain in &config.chains {
        if let Some(ref ws_var) = chain.ws_url_env_var {
            required.push(ws_var.as_str());
        }
    }

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

/// Decode-only mode: runs decoders on existing raw parquet files without
/// collection or transformations. Useful for profiling the decoding pipeline.
async fn decode_only_chain(config: &IndexerConfig, chain: &ChainConfig) -> anyhow::Result<()> {
    tracing::info!("Decode-only processing for chain: {}", chain.name);

    let raw_config = Arc::new(config.raw_data_collection.clone());
    let chain = Arc::new(chain.clone());

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

    let mut tasks: JoinSet<anyhow::Result<()>> = JoinSet::new();

    if has_events {
        tasks.spawn({
            let (chain, cfg) = (chain.clone(), raw_config.clone());
            async move {
                // Create a channel and immediately drop the sender so the decoder
                // runs catchup then exits the live phase immediately.
                let (_tx, rx) = mpsc::channel::<DecoderMessage>(1);
                drop(_tx);
                decode_logs(&chain, &cfg, rx, None, None, None, false)
                    .await
                    .context("log decoding failed")
            }
        });
    }

    if has_calls {
        tasks.spawn({
            let (chain, cfg) = (chain.clone(), raw_config.clone());
            async move {
                let (_tx, rx) = mpsc::channel::<DecoderMessage>(1);
                drop(_tx);
                decode_eth_calls(&chain, &cfg, rx, None, None, None, None, None, false)
                    .await
                    .context("eth call decoding failed")
            }
        });
    }

    if !has_events && !has_calls {
        tracing::warn!("No events or calls configured for chain {}", chain.name);
    }

    while let Some(result) = tasks.join_next().await {
        result.context("decode task panicked")??;
    }

    tracing::info!("Decode-only processing complete for chain {}", chain.name);
    Ok(())
}

/// Live-only mode: skips historical processing and starts directly in live mode.
/// Useful for testing live mode in isolation or running on a dedicated machine.
async fn process_chain_live_only(
    config: &IndexerConfig,
    chain: &ChainConfig,
) -> anyhow::Result<()> {
    tracing::info!("Processing chain {} in live-only mode", chain.name);

    // Validate WebSocket is configured
    if !should_enable_live_mode(config, chain) {
        anyhow::bail!(
            "Live-only mode requires live_mode=true (or not set) and ws_url_env_var configured for chain {}",
            chain.name
        );
    }

    // Load .env if needed for WS URL
    if let Some(ref ws_env_var) = chain.ws_url_env_var {
        if env::var(ws_env_var).is_err() {
            dotenvy::dotenv().ok();
        }
    }

    // Build unified runtime (RPC client with rate limiter, db pool, registry, progress tracker)
    let runtime = ChainRuntime::build(config, chain).await?;

    // Build channels with config-derived capacity
    let channels = CommonChannels::build_for_live_only(
        config,
        &runtime.features,
        runtime.transformations_enabled,
    );

    let mut tasks: JoinSet<anyhow::Result<()>> = JoinSet::new();

    // Spawn transformation engine if enabled
    if runtime.transformations_enabled {
        let rpc_client = Arc::new(runtime.build_additional_client()?);
        let tc = config.transformations.as_ref().unwrap();

        let handler_count = runtime.registry.handler_count();
        let engine = TransformationEngine::new(
            runtime.registry.clone(),
            runtime.db_pool.clone().unwrap(),
            rpc_client,
            chain.name.clone(),
            chain.chain_id,
            ExecutionMode::Streaming, // Live-only mode always uses streaming
            chain.contracts.clone(),
            chain.factory_collections.clone(),
            tc.handler_concurrency,
            runtime.progress_tracker.clone(),
            runtime.features.has_events,
            runtime.features.has_calls,
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

        let transform_events_rx = channels.transform_events_rx;
        let transform_calls_rx = channels.transform_calls_rx;
        let transform_complete_rx = channels.transform_complete_rx;
        let transform_reorg_rx = channels.transform_reorg_rx;
        let transform_retry_rx = channels.transform_retry_rx;

        tasks.spawn(async move {
            engine
                .run(
                    transform_events_rx.unwrap(),
                    transform_calls_rx.unwrap(),
                    transform_complete_rx.unwrap(),
                    transform_reorg_rx,
                    transform_retry_rx,
                    None, // No decode catchup signal in live-only mode
                )
                .await
                .map_err(|e| anyhow::anyhow!("transformation engine error: {}", e))
        });
    }

    // Clone senders before channels are consumed
    let transform_events_tx = channels.transform_events_tx.clone();
    let transform_calls_tx = channels.transform_calls_tx.clone();
    let transform_complete_tx = channels.transform_complete_tx.clone();
    let transform_reorg_tx = channels.transform_reorg_tx.clone();
    let transform_retry_tx = channels.transform_retry_tx.clone();
    let log_decoder_tx = channels.log_decoder_tx.clone();
    let eth_call_decoder_tx = channels.call_decoder_tx.clone();

    // Spawn decoder task for live mode (if events are configured)
    if let Some(rx) = channels.log_decoder_rx {
        let chain = runtime.chain.clone();
        let cfg = config.raw_data_collection.clone();
        let transform_events_tx = transform_events_tx.clone();
        let transform_complete_tx = transform_complete_tx.clone();
        tasks.spawn(async move {
            decode_logs(
                &chain,
                &cfg,
                rx,
                transform_events_tx,
                None,
                transform_complete_tx,
                true,
            )
            .await
            .context("log decoding failed")
        });
    }

    // Spawn eth_call decoder task for live mode (if calls are configured)
    if let Some(rx) = channels.call_decoder_rx {
        let chain = runtime.chain.clone();
        let cfg = config.raw_data_collection.clone();
        let transform_calls_tx = transform_calls_tx.clone();
        let transform_complete_tx = transform_complete_tx.clone();
        let transform_retry_tx = transform_retry_tx.clone();
        tasks.spawn(async move {
            decode_eth_calls(
                &chain,
                &cfg,
                rx,
                transform_calls_tx,
                transform_complete_tx,
                transform_retry_tx,
                None,
                None,
                true,
            )
            .await
            .context("eth_call decoding failed")
        });
    }

    // Initialize storage manager for live-only mode (no retry queue needed)
    let storage_manager = StorageManager::new(config.storage.as_ref(), PathBuf::from("data"), None)
        .await
        .context("failed to initialize storage manager")?;
    let storage_manager = Arc::new(storage_manager);

    // Spawn live mode directly (no historical processing)
    spawn_live_mode(
        runtime.chain.clone(),
        config,
        runtime.http_client.clone(),
        runtime.db_pool.clone(),
        log_decoder_tx,
        eth_call_decoder_tx,
        transform_reorg_tx,
        transform_retry_tx,
        runtime.progress_tracker.clone(),
        Some(storage_manager),
        &mut tasks,
    )
    .await?;

    // Run until shutdown
    while let Some(result) = tasks.join_next().await {
        result.context("live-only task panicked")??;
    }

    tracing::info!("Live-only processing complete for chain {}", chain.name);
    Ok(())
}

async fn process_chain(
    config: &IndexerConfig,
    chain: &ChainConfig,
    storage_manager: Arc<StorageManager>,
) -> anyhow::Result<()> {
    tracing::info!("Processing chain: {}", chain.name);

    let rpc_url = env::var(&chain.rpc_url_env_var).with_context(|| {
        format!(
            "env var {} not set for chain {}",
            chain.rpc_url_env_var, chain.name
        )
    })?;

    // Feature detection
    let features = ChainFeatures::detect(chain, config);
    features.log_summary(&chain.name);

    // Extract feature flags for easier access
    let has_factories = features.has_factories;
    let has_events = features.has_events;
    let has_calls = features.has_calls;
    let has_factory_calls = features.has_factory_calls;
    let has_event_triggered_calls = features.has_event_triggered_calls;
    let needs_factory_filtering = features.needs_factory_filtering;
    let needs_recollect = features.needs_recollect;

    // Channel setup
    let channel_cap = config
        .raw_data_collection
        .channel_capacity
        .unwrap_or(raw_data_defaults::CHANNEL_CAPACITY);
    let factory_cap = config
        .raw_data_collection
        .factory_channel_capacity
        .unwrap_or(raw_data_defaults::FACTORY_CHANNEL_CAPACITY);

    let (block_tx, block_rx) = mpsc::channel(channel_cap);
    let (log_tx, log_rx) = mpsc::channel::<LogMessage>(channel_cap);
    let (eth_call_tx, eth_call_rx) = mpsc::channel(channel_cap);

    let (factory_log_tx, factory_log_rx) =
        optional_channel::<LogMessage>(has_factories, channel_cap);
    let (logs_factory_tx, logs_factory_rx) = optional_channel(needs_factory_filtering, factory_cap);
    let (eth_calls_factory_tx, eth_calls_factory_rx) =
        optional_channel::<FactoryMessage>(has_factory_calls, factory_cap);
    let (event_trigger_tx, event_trigger_rx) =
        optional_channel::<EventTriggerMessage>(has_event_triggered_calls, channel_cap);
    let (log_decoder_tx, log_decoder_rx) =
        optional_channel::<DecoderMessage>(has_events, channel_cap);
    let (call_decoder_tx, call_decoder_rx) =
        optional_channel::<DecoderMessage>(has_calls, channel_cap);
    // Recollect channel is needed by both factory collector and log decoder
    let (recollect_tx, recollect_rx) =
        optional_channel::<RecollectRequest>(needs_recollect, channel_cap);

    // Catchup synchronization barriers:
    // Factory catchup must complete before factory-dependent eth_call catchup
    let (factory_catchup_done_tx, factory_catchup_done_rx) = if has_factories {
        let (tx, rx) = oneshot::channel();
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    // Eth_call catchup must complete before decode catchup
    let (eth_calls_catchup_done_tx, eth_calls_catchup_done_rx) = if has_calls {
        let (tx, rx) = oneshot::channel();
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

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
    let (transform_complete_tx, transform_complete_rx) =
        optional_channel::<RangeCompleteMessage>(transformations_enabled, channel_cap);
    // Reorg channel for live mode cleanup of pending events
    let (transform_reorg_tx, transform_reorg_rx) =
        optional_channel::<ReorgMessage>(transformations_enabled, channel_cap);
    let (transform_retry_tx, transform_retry_rx) =
        optional_channel::<TransformRetryRequest>(transformations_enabled, 100);

    // Decode catchup must complete before transformation engine catchup
    let (decode_catchup_done_tx, decode_catchup_done_rx) = if transformations_enabled && has_calls {
        let (tx, rx) = oneshot::channel();
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

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

    // RPC configuration from per-chain config (not env vars)
    let rpc_concurrency = chain.rpc.concurrency.unwrap_or(rpc_defaults::CONCURRENCY);
    let cu_per_second = chain
        .rpc
        .compute_units_per_second
        .unwrap_or(rpc_defaults::ALCHEMY_CU_PER_SECOND);

    // Use batch size from chain.rpc, falling back to raw_data_collection config, then defaults
    let rpc_batch_size = chain
        .rpc
        .batch_size
        .or(config.raw_data_collection.rpc_batch_size)
        .unwrap_or(rpc_defaults::MAX_BATCH_SIZE);

    // Apply the computed batch size to raw_config so collectors use it
    let mut raw_config = config.raw_data_collection.clone();
    raw_config.rpc_batch_size = Some(rpc_batch_size);
    let raw_config = Arc::new(raw_config);

    tracing::info!(
        "RPC config: concurrency={}, cu_per_second={}, batch_size={}",
        rpc_concurrency,
        cu_per_second,
        rpc_batch_size
    );

    // Create shared rate limiter for account-level rate limiting across all clients
    let shared_limiter = Arc::new(rpc::SlidingWindowRateLimiter::new(cu_per_second));

    // Pre-create RPC clients with shared rate limiter
    let blocks_client = UnifiedRpcClient::from_url_with_options(
        &rpc_url,
        cu_per_second,
        rpc_concurrency,
        Some(shared_limiter.clone()),
    )?;
    let receipts_client = UnifiedRpcClient::from_url_with_options(
        &rpc_url,
        cu_per_second,
        rpc_concurrency,
        Some(shared_limiter.clone()),
    )?;
    let eth_calls_client = UnifiedRpcClient::from_url_with_options(
        &rpc_url,
        cu_per_second,
        rpc_concurrency,
        Some(shared_limiter.clone()),
    )?;

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
    // Clone recollect_tx for both factory collector and log decoder
    let recollect_tx_for_factories = if has_factories {
        recollect_tx.clone()
    } else {
        None
    };
    let recollect_tx_for_log_decoder = if has_events {
        recollect_tx.clone()
    } else {
        None
    };

    // Clone for live mode transition (before originals are moved)
    let log_decoder_tx_for_live = if has_events && should_enable_live_mode(config, &chain) {
        log_decoder_tx.clone()
    } else {
        None
    };

    let eth_call_decoder_tx_for_live = if has_calls && should_enable_live_mode(config, &chain) {
        call_decoder_tx.clone()
    } else {
        None
    };

    // Create HTTP client for live mode (if enabled) - shares rate limiter
    let http_client_for_live = if should_enable_live_mode(config, &chain) {
        Some(Arc::new(build_rpc_client_with_limiter(
            &rpc_url,
            &chain.rpc,
            shared_limiter.clone(),
        )?))
    } else {
        None
    };

    let mut tasks: JoinSet<anyhow::Result<()>> = JoinSet::new();

    tasks.spawn({
        let (chain, cfg) = (chain.clone(), raw_config.clone());
        let s3_manifest = storage_manager.manifest_for(&chain.name);
        let sm = storage_manager.clone();
        async move {
            collect_blocks(
                &chain,
                &blocks_client,
                &cfg,
                Some(block_tx),
                Some(eth_call_tx),
                s3_manifest.as_ref(),
                Some(sm),
            )
            .await
            .context("block collection failed")
        }
    });

    tasks.spawn({
        let (chain, cfg) = (chain.clone(), raw_config.clone());
        let log_tx = Some(log_tx);
        let s3_manifest = storage_manager.manifest_for(&chain.name);
        let sm = storage_manager.clone();
        async move {
            // Catchup: process existing block ranges missing receipts
            let catchup_state = raw_data::historical::catchup::receipts::collect_receipts(
                &chain,
                &receipts_client,
                &cfg,
                &log_tx,
                &factory_log_tx,
                &event_trigger_tx,
                &event_matchers,
                s3_manifest,
                Some(sm.clone()),
            )
            .await
            .context("receipt catchup failed")?;

            // Current: process new blocks from channel
            raw_data::historical::current::receipts::collect_receipts(
                &chain,
                &receipts_client,
                &cfg,
                block_rx,
                log_tx,
                factory_log_tx,
                event_trigger_tx,
                event_matchers,
                recollect_rx,
                catchup_state,
                Some(sm),
            )
            .await
            .context("receipt collection failed")
        }
    });

    tasks.spawn({
        let (chain, cfg) = (chain.clone(), raw_config.clone());
        let s3_manifest = storage_manager.manifest_for(&chain.name);
        let sm = storage_manager.clone();
        async move {
            let catchup_state = raw_data::historical::catchup::logs::collect_logs(
                &chain,
                &cfg,
                s3_manifest,
                Some(sm),
            )
            .await
            .context("log catchup failed")?;

            raw_data::historical::current::logs::collect_logs(
                &chain,
                log_rx,
                logs_factory_rx,
                log_decoder_tx,
                catchup_state,
            )
            .await
            .context("log collection failed")
        }
    });

    tasks.spawn({
        let (chain, cfg) = (chain.clone(), raw_config.clone());
        let s3_manifest = storage_manager.manifest_for(&chain.name);
        let sm = storage_manager.clone();
        async move {
            // Catchup: process existing block/log ranges
            let catchup_state = raw_data::historical::catchup::eth_calls::collect_eth_calls(
                &chain,
                &eth_calls_client,
                &cfg,
                &call_decoder_tx,
                eth_calls_factory_rx.is_some(),
                event_trigger_rx.is_some(),
                factory_catchup_done_rx,
                eth_calls_catchup_done_tx,
                s3_manifest,
                Some(sm.clone()),
            )
            .await
            .context("eth_calls catchup failed")?;

            // Current: process new blocks/factory/event data from channels
            raw_data::historical::current::eth_calls::collect_eth_calls(
                &chain,
                &eth_calls_client,
                eth_call_rx,
                eth_calls_factory_rx,
                event_trigger_rx,
                call_decoder_tx,
                catchup_state,
                Some(sm),
            )
            .await
            .context("eth_calls collection failed")
        }
    });

    if has_factories {
        tasks.spawn({
            let (chain, cfg) = (chain.clone(), raw_config.clone());
            let s3_manifest = storage_manager.manifest_for(&chain.name);
            let sm = storage_manager.clone();
            async move {
                // Catchup: load existing factory data and process gaps
                let catchup_state = raw_data::historical::catchup::factories::collect_factories(
                    &chain,
                    &cfg,
                    &logs_factory_tx,
                    &eth_calls_factory_tx,
                    &log_decoder_tx_for_factories,
                    &call_decoder_tx_for_factories,
                    &recollect_tx_for_factories,
                    factory_catchup_done_tx,
                    s3_manifest,
                    Some(sm),
                )
                .await
                .context("factory catchup failed")?;

                // Current: process new logs from channel
                raw_data::historical::current::factories::collect_factories(
                    &chain,
                    &cfg,
                    factory_log_rx.unwrap(),
                    logs_factory_tx,
                    eth_calls_factory_tx,
                    log_decoder_tx_for_factories,
                    call_decoder_tx_for_factories,
                    catchup_state.matchers,
                    catchup_state.existing_files,
                    catchup_state.output_dir,
                    catchup_state.s3_manifest,
                    catchup_state.storage_manager,
                )
                .await
                .context("factory collection failed")
            }
        });
    }

    if has_events {
        let transform_events_tx = transform_events_tx.clone();
        let transform_complete_tx = transform_complete_tx.clone();
        tasks.spawn({
            let (chain, cfg) = (chain.clone(), raw_config.clone());
            async move {
                decode_logs(
                    &chain,
                    &cfg,
                    log_decoder_rx.unwrap(),
                    transform_events_tx,
                    recollect_tx_for_log_decoder,
                    transform_complete_tx,
                    false,
                )
                .await
                .context("log decoding failed")
            }
        });
    }

    if has_calls {
        let transform_calls_tx = transform_calls_tx.clone();
        let transform_complete_tx = transform_complete_tx.clone();
        let transform_retry_tx = transform_retry_tx.clone();
        tasks.spawn({
            let (chain, cfg) = (chain.clone(), raw_config.clone());
            async move {
                decode_eth_calls(
                    &chain,
                    &cfg,
                    call_decoder_rx.unwrap(),
                    transform_calls_tx,
                    transform_complete_tx,
                    transform_retry_tx,
                    eth_calls_catchup_done_rx,
                    decode_catchup_done_tx,
                    false,
                )
                .await
                .context("eth call decoding failed")
            }
        });
    }

    // Clone db_pool for live mode before it's potentially consumed by transformation engine
    let db_pool_for_live = if should_enable_live_mode(config, &chain) {
        db_pool.clone()
    } else {
        None
    };

    // Create progress tracker for live mode (must be before engine creation so we can pass it)
    let progress_tracker = if should_enable_live_mode(config, &chain) && db_pool.is_some() {
        Some(Arc::new(Mutex::new(LiveProgressTracker::new(
            chain.chain_id as i64,
            db_pool.clone(),
            chain.name.clone(),
        ))))
    } else {
        None
    };

    // Register handlers with progress tracker
    if let Some(ref tracker) = progress_tracker {
        let mut t = tracker.lock().await;
        for handler in registry.all_handlers().iter() {
            t.register_handler(&handler.handler_key());
        }
        tracing::info!(
            "Registered {} handlers with live progress tracker",
            t.handler_keys().len()
        );
    }

    // Create separate JoinSet for tasks that continue into live mode (transformation engine)
    let mut live_tasks: JoinSet<anyhow::Result<()>> = JoinSet::new();

    if transformations_enabled {
        let rpc_client = Arc::new(build_rpc_client_with_limiter(
            &rpc_url,
            &chain.rpc,
            shared_limiter.clone(),
        )?);
        let tc = config.transformations.as_ref().unwrap();
        let mode = if tc.mode.batch_for_catchup {
            ExecutionMode::Batch {
                batch_size: tc.mode.catchup_batch_size,
            }
        } else {
            ExecutionMode::Streaming
        };

        let handler_count = registry.handler_count();
        let retry_rx = transform_retry_rx;
        let engine = TransformationEngine::new(
            Arc::new(registry),
            db_pool.unwrap(),
            rpc_client,
            chain.name.clone(),
            chain.chain_id,
            mode,
            chain.contracts.clone(),
            chain.factory_collections.clone(),
            tc.handler_concurrency,
            progress_tracker.clone(),
            has_events,
            has_calls,
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

        // Spawn into live_tasks so engine continues running into live mode
        live_tasks.spawn(async move {
            engine
                .run(
                    transform_events_rx.unwrap(),
                    transform_calls_rx.unwrap(),
                    transform_complete_rx.unwrap(),
                    transform_reorg_rx,
                    retry_rx,
                    decode_catchup_done_rx,
                )
                .await
                .map_err(|e| anyhow::anyhow!("transformation engine error: {}", e))
        });
    }

    while let Some(result) = tasks.join_next().await {
        result.context("pipeline task panicked")??;
    }

    tracing::info!("Completed historical processing for chain {}", chain.name);

    // Transition to live mode (default behavior unless explicitly disabled)
    if should_enable_live_mode(config, &chain) {
        tracing::info!("Historical processing complete, transitioning to live mode");
        spawn_live_mode(
            chain.clone(),
            config,
            http_client_for_live.unwrap(),
            db_pool_for_live,
            log_decoder_tx_for_live,
            eth_call_decoder_tx_for_live,
            transform_reorg_tx,
            transform_retry_tx,
            progress_tracker,
            Some(storage_manager.clone()),
            &mut live_tasks,
        )
        .await?;

        // Wait for live mode tasks including the transformation engine
        while let Some(result) = live_tasks.join_next().await {
            result.context("live mode task panicked")??;
        }
    } else {
        tracing::info!("Live mode not enabled, exiting after historical processing");
        // If not entering live mode, wait for the transformation engine to finish
        while let Some(result) = live_tasks.join_next().await {
            result.context("transformation engine task panicked")??;
        }
    }

    Ok(())
}

/// Check if live mode should be enabled for a chain.
///
/// Live mode is enabled by default and only disabled if:
/// - `live_mode` is explicitly set to `false` in config
/// - `ws_url_env_var` is not set in chain config
/// - The WebSocket URL environment variable is not set
fn should_enable_live_mode(config: &IndexerConfig, chain: &ChainConfig) -> bool {
    // Live mode is enabled by default, only disabled if explicitly set to false
    let live_mode_enabled = config.raw_data_collection.live_mode.unwrap_or(true);

    if !live_mode_enabled {
        tracing::info!("Live mode explicitly disabled for chain {}", chain.name);
        return false;
    }

    // Require WebSocket URL to be configured
    let Some(ref ws_env_var) = chain.ws_url_env_var else {
        tracing::debug!(
            "Live mode skipped: ws_url_env_var not set for chain {}",
            chain.name
        );
        return false;
    };

    if env::var(ws_env_var).is_err() {
        tracing::debug!(
            "Live mode skipped: {} env var not set for chain {}",
            ws_env_var,
            chain.name
        );
        return false;
    }

    true
}

/// Spawn live mode tasks for a chain.
///
/// This sets up:
/// - WebSocket client subscription
/// - Live block collector
/// - Compaction service
///
/// Note: The transformation engine is NOT spawned here - it continues running from
/// historical mode via the live_tasks JoinSet, receiving messages via the same channels.
async fn spawn_live_mode(
    chain: Arc<ChainConfig>,
    config: &IndexerConfig,
    http_client: Arc<UnifiedRpcClient>,
    db_pool: Option<Arc<DbPool>>,
    log_decoder_tx: Option<mpsc::Sender<DecoderMessage>>,
    eth_call_decoder_tx: Option<mpsc::Sender<DecoderMessage>>,
    transform_reorg_tx: Option<mpsc::Sender<ReorgMessage>>,
    transform_retry_tx: Option<mpsc::Sender<TransformRetryRequest>>,
    progress_tracker: Option<Arc<Mutex<LiveProgressTracker>>>,
    storage_manager: Option<Arc<StorageManager>>,
    tasks: &mut JoinSet<anyhow::Result<()>>,
) -> anyhow::Result<()> {
    let ws_env_var = chain
        .ws_url_env_var
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("ws_url_env_var not set"))?;

    let ws_url = env::var(ws_env_var)
        .with_context(|| format!("env var {} not set for chain {}", ws_env_var, chain.name))?;

    // Use batch size from chain.rpc, falling back to raw_data_collection config, then defaults
    let rpc_batch_size = chain
        .rpc
        .batch_size
        .or(config.raw_data_collection.rpc_batch_size)
        .unwrap_or(rpc_defaults::MAX_BATCH_SIZE);

    // Build live mode config
    let live_config = LiveModeConfig {
        reorg_depth: config
            .raw_data_collection
            .reorg_depth
            .unwrap_or(raw_data_defaults::REORG_DEPTH),
        compaction_interval_secs: config
            .raw_data_collection
            .compaction_interval_secs
            .unwrap_or(raw_data_defaults::COMPACTION_INTERVAL_SECS),
        range_size: config
            .raw_data_collection
            .parquet_block_range
            .unwrap_or(1000) as u64,
        transform_retry_grace_period_secs: config
            .raw_data_collection
            .transform_retry_grace_period_secs
            .unwrap_or(raw_data_defaults::TRANSFORM_RETRY_GRACE_PERIOD_SECS),
    };

    tracing::info!(
        "Starting live mode for chain {} with reorg_depth={}, compaction_interval={}s",
        chain.name,
        live_config.reorg_depth,
        live_config.compaction_interval_secs
    );

    // Create WebSocket client
    let ws_client = Arc::new(
        WsClient::from_urls(&ws_url, http_client.clone())
            .map_err(|e| anyhow::anyhow!("Failed to create WebSocket client: {}", e))?,
    );

    // Create channels
    let (ws_event_tx, ws_event_rx) = mpsc::unbounded_channel();
    let (live_msg_tx, mut live_msg_rx) = mpsc::channel::<LiveMessage>(1000);

    // Drain LiveMessage channel to prevent backpressure on the collector.
    // Note: Transformations are handled via the transformation engine's channels
    // (transform_events_tx, transform_calls_tx, etc.), not via LiveMessage.
    tasks.spawn(async move {
        while live_msg_rx.recv().await.is_some() {}
        Ok(())
    });

    // Use provided progress tracker or create a fallback (for live-only mode)
    let progress_tracker = progress_tracker.unwrap_or_else(|| {
        Arc::new(Mutex::new(LiveProgressTracker::new(
            chain.chain_id as i64,
            db_pool.clone(),
            chain.name.clone(),
        )))
    });

    // Build factory matchers for live mode factory address extraction
    let factory_matchers = Arc::new(build_factory_matchers(&chain.contracts));
    if !factory_matchers.is_empty() {
        tracing::info!(
            "Built {} factory matchers for live mode",
            factory_matchers.len()
        );
    }

    // Start WebSocket subscription
    let ws_handle = ws_client.clone().subscribe(ws_event_tx);
    tasks.spawn(async move {
        ws_handle
            .await
            .map_err(|e| anyhow::anyhow!("WebSocket task panicked: {:?}", e))?
            .map_err(|e| anyhow::anyhow!("WebSocket error: {}", e))
    });

    // Build eth_call collector if eth_calls are configured
    let eth_call_collector = if eth_call_decoder_tx.is_some() {
        // Get multicall3 address from contracts if available
        let multicall3_address = chain.contracts.get("Multicall3").and_then(|c| {
            use crate::types::config::contract::AddressOrAddresses;
            match &c.address {
                AddressOrAddresses::Single(addr) => Some(*addr),
                AddressOrAddresses::Multiple(addrs) => addrs.first().copied(),
            }
        });
        let collector = LiveEthCallCollector::new(
            &chain,
            http_client.clone(),
            multicall3_address,
            rpc_batch_size as usize,
        );
        if collector.has_calls() {
            Some(collector)
        } else {
            None
        }
    } else {
        None
    };

    let live_expectations = live_pipeline_expectations(
        log_decoder_tx.is_some(),
        eth_call_collector.is_some(),
        eth_call_decoder_tx.is_some() && eth_call_collector.is_some(),
        transform_retry_tx.is_some(),
    );

    // Start live collector
    let collector = LiveCollector::new(
        chain.clone(),
        http_client.clone(),
        live_config.clone(),
        Some(progress_tracker.clone()),
        factory_matchers,
        eth_call_collector,
        db_pool.clone(),
        live_expectations,
        transform_retry_tx.clone(),
    );
    tasks.spawn(async move {
        collector
            .run(
                ws_event_rx,
                live_msg_tx,
                log_decoder_tx,
                eth_call_decoder_tx,
                transform_reorg_tx,
            )
            .await
            .map_err(|e| anyhow::anyhow!("Live collector error: {}", e))
    });

    // Start compaction service with retry channel
    let mut compaction = CompactionService::new(
        chain.name.clone(),
        chain.chain_id,
        live_config,
        db_pool,
        progress_tracker,
        live_expectations,
        storage_manager,
    );
    if let Some(retry_tx) = transform_retry_tx {
        compaction = compaction.with_retry_tx(retry_tx);
    }
    tasks.spawn(async move {
        compaction.run().await;
        Ok(())
    });

    Ok(())
}
