#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

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

use std::collections::{HashMap, HashSet};
use std::env;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinSet;
use tracing_subscriber::EnvFilter;

use db::DbPool;
use decoding::{decode_eth_calls, decode_logs, DecoderMessage};
use live::{
    CompactionService, LiveCollector, LiveCollectorConfig, LiveEthCallCollector, LiveMessage,
    LiveModeConfig, LivePipelineExpectations, LiveProgressTracker, TransformRetryRequest,
};
use raw_data::historical::catchup::blocks::collect_blocks;
use raw_data::historical::factories::{
    build_factory_matchers, FactoryAddressData, FactoryMessage, RecollectRequest,
};
use raw_data::historical::receipts::{
    build_event_trigger_matchers, EventTriggerMessage, LogMessage,
};
use rpc::{SlidingWindowRateLimiter, UnifiedRpcClient, WsClient};
use runtime::{
    build_rpc_client, build_rpc_client_with_limiter, ChainFeatures, ChainRuntime, CommonChannels,
};
use storage::{InitialSyncService, LocalBackend, RetryQueue, S3Backend, StorageManager};
use transformations::{
    ExecutionMode, ReorgMessage, TransformationEngine, TransformationEngineConfig,
};
use types::config::chain::ChainConfig;
use types::config::defaults::{raw_data as raw_data_defaults, rpc as rpc_defaults};
use types::config::indexer::IndexerConfig;
use types::config::raw_data::RawDataCollectionConfig;
use types::shared::repair::RepairScope;

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

fn collect_flag_values(args: &[String], flag: &str) -> anyhow::Result<Vec<String>> {
    let mut values = Vec::new();
    let mut idx = 0usize;

    while idx < args.len() {
        let arg = &args[idx];
        if arg == flag {
            let Some(next) = args.get(idx + 1) else {
                anyhow::bail!("Missing value for {}", flag);
            };
            if next.starts_with("--") {
                anyhow::bail!("Missing value for {}", flag);
            }
            values.extend(
                next.split(',')
                    .map(str::trim)
                    .filter(|v| !v.is_empty())
                    .map(ToString::to_string),
            );
            idx += 2;
            continue;
        }

        let prefix = format!("{}=", flag);
        if let Some(raw) = arg.strip_prefix(&prefix) {
            values.extend(
                raw.split(',')
                    .map(str::trim)
                    .filter(|v| !v.is_empty())
                    .map(ToString::to_string),
            );
        }

        idx += 1;
    }

    Ok(values)
}

/// Parsed per-chain and global block overrides from CLI.
struct BlockOverrides {
    global_from: Option<u64>,
    global_to: Option<u64>,
    per_chain_from: HashMap<String, u64>,
    per_chain_to: HashMap<String, u64>,
}

fn parse_block_overrides(args: &[String]) -> anyhow::Result<BlockOverrides> {
    let from_values = collect_flag_values(args, "--from-block")?;
    let to_values = collect_flag_values(args, "--to-block")?;

    let mut global_from: Option<u64> = None;
    let mut per_chain_from: HashMap<String, u64> = HashMap::new();

    for val in &from_values {
        if let Some((chain, block_str)) = val.split_once(':') {
            let block = block_str.parse::<u64>().with_context(|| {
                format!(
                    "Invalid numeric value '{}' for --from-block (chain '{}')",
                    block_str, chain
                )
            })?;
            per_chain_from.insert(chain.to_string(), block);
        } else {
            let block = val
                .parse::<u64>()
                .with_context(|| format!("Invalid numeric value '{}' for --from-block", val))?;
            global_from = Some(block);
        }
    }

    let mut global_to: Option<u64> = None;
    let mut per_chain_to: HashMap<String, u64> = HashMap::new();

    for val in &to_values {
        if let Some((chain, block_str)) = val.split_once(':') {
            let block = block_str.parse::<u64>().with_context(|| {
                format!(
                    "Invalid numeric value '{}' for --to-block (chain '{}')",
                    block_str, chain
                )
            })?;
            per_chain_to.insert(chain.to_string(), block);
        } else {
            let block = val
                .parse::<u64>()
                .with_context(|| format!("Invalid numeric value '{}' for --to-block", val))?;
            global_to = Some(block);
        }
    }

    // Validate global from <= to when both set
    if let (Some(from), Some(to)) = (global_from, global_to) {
        anyhow::ensure!(
            from <= to,
            "--from-block ({}) cannot be greater than --to-block ({})",
            from,
            to
        );
    }

    Ok(BlockOverrides {
        global_from,
        global_to,
        per_chain_from,
        per_chain_to,
    })
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
    let catch_up_only = args.iter().any(|a| a == "--catch-up-only");
    let repair_only = args.iter().any(|a| a == "--repair-only");
    let repair_factories = args.iter().any(|a| a == "--repair-factories");
    let repair_flag = args.iter().any(|a| a == "--repair");
    let transformation_only = args.iter().any(|a| a == "--transformation-only");
    let repair = repair_flag || repair_only;
    let block_overrides = parse_block_overrides(&args)?;
    let parsed_sources = collect_flag_values(&args, "--source")?;
    let parsed_functions = collect_flag_values(&args, "--function")?;

    let transformation_handlers: Option<HashSet<String>> = {
        let values = collect_flag_values(&args, "--transformation-handlers")?;
        if values.is_empty() {
            None
        } else {
            Some(values.into_iter().collect())
        }
    };

    if live_only && decode_only {
        anyhow::bail!("Cannot use --live-only and --decode-only together");
    }
    if catch_up_only && live_only {
        anyhow::bail!("Cannot use --catch-up-only and --live-only together");
    }
    if catch_up_only && decode_only {
        anyhow::bail!("Cannot use --catch-up-only and --decode-only together");
    }
    if repair_only && live_only {
        anyhow::bail!("Cannot use --repair-only and --live-only together");
    }
    if repair_only && decode_only {
        anyhow::bail!("Cannot use --repair-only and --decode-only together");
    }
    if repair_only && catch_up_only {
        anyhow::bail!("Cannot use --repair-only and --catch-up-only together");
    }
    if repair_factories && live_only {
        anyhow::bail!("Cannot use --repair-factories and --live-only together");
    }
    if repair_factories && decode_only {
        anyhow::bail!("Cannot use --repair-factories and --decode-only together");
    }
    if repair_factories && catch_up_only {
        anyhow::bail!("Cannot use --repair-factories and --catch-up-only together");
    }
    if repair_factories && repair_only {
        anyhow::bail!("Cannot use --repair-factories and --repair-only together");
    }
    if repair_factories && repair_flag {
        anyhow::bail!("Cannot use --repair-factories and --repair together");
    }
    if repair_flag && live_only {
        anyhow::bail!("Cannot use --repair and --live-only together");
    }
    if transformation_only && decode_only {
        anyhow::bail!("Cannot use --transformation-only and --decode-only together");
    }
    if transformation_only && live_only {
        anyhow::bail!("Cannot use --transformation-only and --live-only together");
    }
    if transformation_only && catch_up_only {
        anyhow::bail!("Cannot use --transformation-only and --catch-up-only together");
    }
    if transformation_only && repair_only {
        anyhow::bail!("Cannot use --transformation-only and --repair-only together");
    }
    if transformation_only && repair_flag {
        anyhow::bail!("Cannot use --transformation-only and --repair together");
    }
    let has_source_or_function = args
        .iter()
        .any(|a| a == "--source" || a.starts_with("--source="))
        || args
            .iter()
            .any(|a| a == "--function" || a.starts_with("--function="));
    if has_source_or_function && repair_factories {
        anyhow::bail!("--source/--function are not supported with --repair-factories");
    }
    if has_source_or_function && !repair {
        anyhow::bail!("--source/--function require --repair or --repair-only");
    }

    let chains_filter: Option<Vec<String>> = args
        .iter()
        .position(|a| a == "--chains")
        .and_then(|i| args.get(i + 1))
        .map(|v| v.split(',').map(|s| s.trim().to_string()).collect())
        .or_else(|| {
            args.iter().find(|a| a.starts_with("--chains=")).map(|a| {
                a.trim_start_matches("--chains=")
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .collect()
            })
        });

    let mut config = IndexerConfig::load(Path::new("config/config.json"))?;

    if let Some(ref filter) = chains_filter {
        for name in filter {
            if !config.chains.iter().any(|c| c.name == *name) {
                let available: Vec<&str> = config.chains.iter().map(|c| c.name.as_str()).collect();
                anyhow::bail!(
                    "Unknown chain '{}' in --chains filter. Available chains: {}",
                    name,
                    available.join(", ")
                );
            }
        }
        let before = config.chains.len();
        config.chains.retain(|c| filter.contains(&c.name));
        tracing::info!(
            "Filtered to {} of {} configured chains: {:?}",
            config.chains.len(),
            before,
            filter
        );
    }

    // Merge CLI block range overrides into chain configs
    for chain in &mut config.chains {
        // Priority: per-chain CLI > global CLI > config JSON
        chain.from_block = block_overrides
            .per_chain_from
            .get(&chain.name)
            .copied()
            .or(block_overrides.global_from)
            .or(chain.from_block);
        chain.to_block = block_overrides
            .per_chain_to
            .get(&chain.name)
            .copied()
            .or(block_overrides.global_to)
            .or(chain.to_block);
    }

    // Validate per-chain from <= to after merging
    for chain in &config.chains {
        if let (Some(from), Some(to)) = (chain.from_block, chain.to_block) {
            anyhow::ensure!(
                from <= to,
                "from_block ({}) cannot be greater than to_block ({}) for chain '{}'",
                from,
                to,
                chain.name
            );
        }
    }

    if !decode_only && !repair_factories {
        let require_ws = !catch_up_only && !repair_only && !transformation_only;
        load_required_env_vars(&config, require_ws)?;
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
        metrics::describe_db_metrics();
        metrics::describe_transformation_metrics();
        metrics::describe_collection_metrics();
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

    // Start periodic manifest refresh if S3 is enabled
    if storage_manager.is_s3_enabled() {
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
    } else if catch_up_only {
        tracing::info!("Running in catch-up-only mode (fills gaps in existing data, no live mode)");
    } else if repair_factories {
        tracing::info!(
            "Running in repair-factories mode (rebuild factory parquet and contract indexes from raw logs)"
        );
    } else if transformation_only {
        tracing::info!("Running in transformation-only mode (transformation catchup only, no collection/decoding/live)");
    }
    if let Some(ref handlers) = transformation_handlers {
        tracing::info!("Transformation handler filter: {:?}", handlers);
    }
    if repair_only {
        tracing::info!("Running in repair-only mode (eth_call repair passes only, no live mode)");
    } else if repair {
        tracing::info!(
            "Repair mode enabled: rebuilding once indexes from parquet, auditing raw-vs-decoded once schema drift, and repairing legacy factory-once sidecars"
        );
    }
    for chain in &config.chains {
        if chain.from_block.is_some() || chain.to_block.is_some() {
            tracing::info!(
                "Chain {} block range override: from={:?}, to={:?}",
                chain.name,
                chain.from_block,
                chain.to_block
            );
        }
    }

    tracing::info!("Loaded config with {} chain(s)", config.chains.len());

    let shared_db_pool: Option<Arc<DbPool>> = if !decode_only && !repair_only && !repair_factories {
        if let Some(ref tc) = config.transformations {
            let registry = transformations::build_registry(0, transformation_handlers.as_ref());
            if !registry.is_empty() {
                let database_url = std::env::var(&tc.database_url_env_var).with_context(|| {
                    format!(
                        "env var {} not set for transformations",
                        tc.database_url_env_var
                    )
                })?;
                let pool = DbPool::new(&database_url, tc.db_pool_size)
                    .await
                    .context("failed to create shared database pool")?;
                pool.run_migrations()
                    .await
                    .context("failed to run database migrations")?;
                pool.run_handler_migrations(&registry)
                    .await
                    .context("failed to run handler migrations")?;
                tracing::info!("Shared database pool initialized and migrations complete");
                Some(Arc::new(pool))
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    };

    // Spawn pool metrics sampler if we have a shared pool
    if let Some(ref pool) = shared_db_pool {
        tokio::spawn(metrics::sample_pool_stats(
            Arc::clone(pool),
            std::time::Duration::from_secs(5),
        ));
    }

    // Build shared rate limiters for rate_limit_group references.
    // Chains that share a group (e.g. "alchemy") will share a single limiter instance,
    // ensuring the aggregate CU/s across all chains stays within the account-level budget.
    let shared_rate_limiters: HashMap<String, Arc<SlidingWindowRateLimiter>> = config
        .rpc_rate_limits
        .as_ref()
        .map(|groups| {
            groups
                .iter()
                .map(|(name, group)| {
                    tracing::info!(
                        "Created shared rate limiter for group '{}': {} units/s",
                        name,
                        group.units_per_second
                    );
                    (
                        name.clone(),
                        Arc::new(SlidingWindowRateLimiter::new(group.units_per_second)),
                    )
                })
                .collect()
        })
        .unwrap_or_default();

    let mut chain_tasks: JoinSet<(String, anyhow::Result<()>)> = JoinSet::new();

    for chain in &config.chains {
        let chain_name = chain.name.clone();
        let config = config.clone();
        let storage_manager = storage_manager.clone();
        let shared_db_pool = shared_db_pool.clone();
        let chain = chain.clone();
        let transformation_handlers = transformation_handlers.clone();

        // Build per-chain repair scope from the chain's merged block overrides
        let repair_scope = if repair {
            let scope = RepairScope {
                from_block: chain.from_block,
                to_block: chain.to_block,
                sources: (!parsed_sources.is_empty())
                    .then(|| parsed_sources.iter().cloned().collect()),
                functions: (!parsed_functions.is_empty())
                    .then(|| parsed_functions.iter().cloned().collect()),
            };
            if scope.is_unscoped() {
                None
            } else {
                Some(scope)
            }
        } else {
            None
        };

        // Look up shared rate limiter for this chain's group
        let shared_rate_limiter = chain
            .rpc
            .rate_limit_group
            .as_ref()
            .and_then(|group| shared_rate_limiters.get(group).cloned());

        chain_tasks.spawn(async move {
            let result = if transformation_only {
                transformation_only_chain(
                    &config,
                    &chain,
                    shared_db_pool,
                    transformation_handlers.as_ref(),
                )
                .await
            } else if live_only {
                process_chain_live_only(
                    &config,
                    &chain,
                    shared_db_pool,
                    shared_rate_limiter,
                    transformation_handlers.as_ref(),
                )
                .await
            } else if repair_only {
                repair_only_chain(
                    &config,
                    &chain,
                    storage_manager,
                    repair_scope,
                    shared_rate_limiter,
                )
                .await
            } else if repair_factories {
                repair_factories_chain(&config, &chain, storage_manager).await
            } else if decode_only {
                decode_only_chain(&config, &chain, repair, repair_scope).await
            } else {
                process_chain(
                    &config,
                    &chain,
                    storage_manager,
                    catch_up_only,
                    repair,
                    repair_scope,
                    shared_db_pool,
                    shared_rate_limiter,
                    transformation_handlers.as_ref(),
                )
                .await
            };
            (chain_name, result)
        });
    }

    let mut failures: Vec<(String, String)> = Vec::new();

    while let Some(result) = chain_tasks.join_next().await {
        match result {
            Ok((chain_name, Ok(()))) => {
                tracing::info!("Chain {} completed successfully", chain_name);
            }
            Ok((chain_name, Err(e))) => {
                tracing::error!("Chain {} failed: {:?}", chain_name, e);
                failures.push((chain_name, format!("{:?}", e)));
            }
            Err(join_error) => {
                tracing::error!("Chain task panicked: {:?}", join_error);
                failures.push(("unknown".to_string(), format!("{:?}", join_error)));
            }
        }
    }

    if !failures.is_empty() {
        for (name, err) in &failures {
            tracing::error!("  Chain '{}': {}", name, err);
        }
        anyhow::bail!(
            "{} chain(s) failed: {}",
            failures.len(),
            failures
                .iter()
                .map(|(n, _)| n.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    Ok(())
}

/// Ensures all required RPC/WS URL env vars are set, loading .env if needed.
fn load_required_env_vars(config: &IndexerConfig, require_ws: bool) -> anyhow::Result<()> {
    let mut required: Vec<&str> = config
        .chains
        .iter()
        .map(|c| c.rpc_url_env_var.as_str())
        .collect();

    if require_ws {
        for chain in &config.chains {
            if let Some(ref ws_var) = chain.ws_url_env_var {
                required.push(ws_var.as_str());
            }
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
async fn decode_only_chain(
    config: &IndexerConfig,
    chain: &ChainConfig,
    repair: bool,
    repair_scope: Option<RepairScope>,
) -> anyhow::Result<()> {
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
            let repair_scope = repair_scope.clone();
            async move {
                let (_tx, rx) = mpsc::channel::<DecoderMessage>(1);
                drop(_tx);
                let outputs = decoding::EthCallDecoderOutputs {
                    transform_tx: None,
                    complete_tx: None,
                    retry_tx: None,
                };
                decode_eth_calls(
                    &chain,
                    &cfg,
                    rx,
                    outputs,
                    None,
                    None,
                    false,
                    repair,
                    repair_scope,
                )
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

/// Transformation-only mode: runs transformation catchup on existing decoded
/// parquet files without any collection, decoding, or live mode.
///
/// Useful for re-running transformations after handler changes without
/// re-collecting or re-decoding raw data.
async fn transformation_only_chain(
    config: &IndexerConfig,
    chain: &ChainConfig,
    shared_db_pool: Option<Arc<DbPool>>,
    handler_filter: Option<&HashSet<String>>,
) -> anyhow::Result<()> {
    tracing::info!("Transformation-only processing for chain: {}", chain.name);

    let tc = config.transformations.as_ref().ok_or_else(|| {
        anyhow::anyhow!("transformations config required for --transformation-only mode")
    })?;

    // Build handler registry filtered to this chain (and optionally by handler names)
    let registry = transformations::build_registry_for_chain(
        chain.chain_id,
        &chain.contracts,
        &chain.factory_collections,
        handler_filter,
    );

    if registry.is_empty() {
        tracing::info!(
            "No transformation handlers registered for chain {}, nothing to do",
            chain.name
        );
        return Ok(());
    }

    let registry = Arc::new(registry);

    // Setup database pool
    let db_pool = if let Some(pool) = shared_db_pool {
        pool
    } else {
        let database_url = std::env::var(&tc.database_url_env_var).with_context(|| {
            format!(
                "env var {} not set for transformations",
                tc.database_url_env_var
            )
        })?;
        let pool = DbPool::new(&database_url, tc.db_pool_size)
            .await
            .context("failed to create database pool")?;
        pool.run_migrations()
            .await
            .context("failed to run database migrations")?;
        pool.run_handler_migrations(&registry)
            .await
            .context("failed to run handler migrations")?;
        Arc::new(pool)
    };

    // Build a minimal RPC client (needed by transformation engine for ad-hoc queries).
    let rpc_url = std::env::var(&chain.rpc_url_env_var).with_context(|| {
        format!(
            "env var {} not set for chain {} (needed for transformation ad-hoc RPC queries)",
            chain.rpc_url_env_var, chain.name
        )
    })?;
    let rpc_batch_size = chain
        .rpc
        .batch_size
        .or(config.raw_data_collection.rpc_batch_size)
        .unwrap_or(types::config::defaults::rpc::MAX_BATCH_SIZE) as usize;
    let (_rate_limiter, rpc_client) = build_rpc_client(&rpc_url, &chain.rpc, rpc_batch_size)?;

    // Validate call dependencies
    transformations::registry::validate_call_dependencies(
        &registry,
        &chain.contracts,
        &chain.factory_collections,
    );

    let mode = if tc.mode.batch_for_catchup {
        ExecutionMode::Batch {
            batch_size: tc.mode.catchup_batch_size,
        }
    } else {
        ExecutionMode::Streaming
    };

    let engine = TransformationEngine::new(
        registry.clone(),
        db_pool,
        rpc_client,
        TransformationEngineConfig {
            chain_name: chain.name.clone(),
            chain_id: chain.chain_id,
            mode,
            contracts: chain.contracts.clone(),
            factory_collections: chain.factory_collections.clone(),
            handler_concurrency: tc.handler_concurrency,
            expect_log_completion: false,
            expect_eth_call_completion: false,
            from_block: chain.from_block,
            to_block: chain.to_block,
        },
        None, // No live progress tracker
    )
    .await
    .context("failed to create transformation engine")?;

    engine
        .initialize()
        .await
        .context("failed to initialize transformation handlers")?;

    tracing::info!(
        "Transformation engine initialized for chain {} with {} handlers, running catchup",
        chain.name,
        registry.handler_count()
    );

    engine
        .run_catchup()
        .await
        .map_err(|e| anyhow::anyhow!("transformation catchup error: {}", e))?;

    tracing::info!(
        "Transformation-only processing complete for chain {}",
        chain.name
    );
    Ok(())
}

/// Repair-only mode: run eth_call repair passes on existing raw/decoded files
/// without normal collection, transformations, or live mode.
async fn repair_only_chain(
    config: &IndexerConfig,
    chain: &ChainConfig,
    storage_manager: Arc<StorageManager>,
    repair_scope: Option<RepairScope>,
    shared_rate_limiter: Option<Arc<SlidingWindowRateLimiter>>,
) -> anyhow::Result<()> {
    tracing::info!("Repair-only processing for chain: {}", chain.name);

    let features = ChainFeatures::detect(chain, config);
    features.log_summary(&chain.name);

    if !features.has_calls {
        tracing::info!(
            "No eth_calls configured for chain {}, nothing to repair",
            chain.name
        );
        return Ok(());
    }

    let rpc_url = std::env::var(&chain.rpc_url_env_var).with_context(|| {
        format!(
            "env var {} not set for chain {}",
            chain.rpc_url_env_var, chain.name
        )
    })?;
    let rpc_batch_size = chain
        .rpc
        .batch_size
        .or(config.raw_data_collection.rpc_batch_size)
        .unwrap_or(rpc_defaults::MAX_BATCH_SIZE) as usize;
    let (_rate_limiter, client) = if let Some(limiter) = shared_rate_limiter {
        let client =
            build_rpc_client_with_limiter(&rpc_url, &chain.rpc, rpc_batch_size, limiter.clone())?;
        (limiter, Arc::new(client))
    } else {
        build_rpc_client(&rpc_url, &chain.rpc, rpc_batch_size)?
    };

    let s3_manifest = storage_manager.manifest_for(&chain.name);

    raw_data::historical::catchup::eth_calls::collect_eth_calls(
        chain,
        client.as_ref(),
        &config.raw_data_collection,
        true,
        true,
        repair_scope.clone(),
        features.has_factory_calls,
        features.has_event_triggered_calls,
        None,
        None,
        s3_manifest,
        Some(storage_manager),
    )
    .await
    .context("eth_calls raw repair failed")?;

    let (_tx, rx) = mpsc::channel::<DecoderMessage>(1);
    drop(_tx);
    let outputs = decoding::EthCallDecoderOutputs {
        transform_tx: None,
        complete_tx: None,
        retry_tx: None,
    };
    decode_eth_calls(
        chain,
        &config.raw_data_collection,
        rx,
        outputs,
        None,
        None,
        false,
        true,
        repair_scope,
    )
    .await
    .context("eth_call decode repair failed")?;

    tracing::info!("Repair-only processing complete for chain {}", chain.name);
    Ok(())
}

/// Repair-factories mode: rebuild factory parquet and contract indexes from
/// existing raw log parquet without running decoding, transformations, or live mode.
async fn repair_factories_chain(
    config: &IndexerConfig,
    chain: &ChainConfig,
    storage_manager: Arc<StorageManager>,
) -> anyhow::Result<()> {
    tracing::info!("Repair-factories processing for chain: {}", chain.name);

    let has_factories = chain.contracts.values().any(|c| has_items(&c.factories));
    if !has_factories {
        tracing::info!(
            "No factory configs for chain {}, nothing to repair",
            chain.name
        );
        return Ok(());
    }

    let s3_manifest = storage_manager.manifest_for(&chain.name);
    raw_data::historical::catchup::factories::collect_factories(
        chain,
        &config.raw_data_collection,
        &None,
        &None,
        &None,
        None,
        s3_manifest,
        Some(storage_manager),
        true,
    )
    .await
    .context("factory repair failed")?;

    tracing::info!(
        "Repair-factories processing complete for chain {}",
        chain.name
    );
    Ok(())
}

/// Live-only mode: skips historical processing and starts directly in live mode.
/// Useful for testing live mode in isolation or running on a dedicated machine.
async fn process_chain_live_only(
    config: &IndexerConfig,
    chain: &ChainConfig,
    shared_db_pool: Option<Arc<DbPool>>,
    shared_rate_limiter: Option<Arc<SlidingWindowRateLimiter>>,
    handler_filter: Option<&HashSet<String>>,
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
    let runtime = ChainRuntime::build(
        config,
        chain,
        shared_db_pool,
        shared_rate_limiter,
        handler_filter,
    )
    .await?;

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
            TransformationEngineConfig {
                chain_name: chain.name.clone(),
                chain_id: chain.chain_id,
                mode: ExecutionMode::Streaming, // Live-only mode always uses streaming
                contracts: chain.contracts.clone(),
                factory_collections: chain.factory_collections.clone(),
                handler_concurrency: tc.handler_concurrency,
                expect_log_completion: runtime.features.has_events,
                expect_eth_call_completion: runtime.features.has_calls,
                from_block: chain.from_block,
                to_block: chain.to_block,
            },
            runtime.progress_tracker.clone(),
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
            let outputs = decoding::EthCallDecoderOutputs {
                transform_tx: transform_calls_tx.as_ref(),
                complete_tx: transform_complete_tx.as_ref(),
                retry_tx: transform_retry_tx.as_ref(),
            };
            decode_eth_calls(&chain, &cfg, rx, outputs, None, None, true, false, None)
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
        LiveModeChannels {
            log_decoder_tx,
            eth_call_decoder_tx,
            transform_reorg_tx,
            transform_retry_tx,
        },
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

/// Spawn a two-phase (catchup then current) stage into a JoinSet.
///
/// `catchup_fut` runs first and produces a state value.
/// `current_fn` receives that state and returns a future for the current phase.
///
/// The current phase always runs, even in catch-up-only mode. In that mode,
/// upstream senders (block_tx, eth_call_tx) are pre-dropped so the current
/// phases see closed input channels and exit promptly — but they must still
/// run to drain any work enqueued by the catchup phase (e.g. LogMessages)
/// and write the corresponding output artifacts.
fn spawn_two_phase_async<S, CatchupFut, CurrentFut, CurrentFn>(
    tasks: &mut JoinSet<anyhow::Result<()>>,
    catchup_fut: CatchupFut,
    current_fn: CurrentFn,
    stage_name: &'static str,
) where
    S: Send + 'static,
    CatchupFut: Future<Output = anyhow::Result<S>> + Send + 'static,
    CurrentFut: Future<Output = anyhow::Result<()>> + Send + 'static,
    CurrentFn: FnOnce(S) -> CurrentFut + Send + 'static,
{
    tasks.spawn(async move {
        let state = catchup_fut
            .await
            .with_context(|| format!("{} catchup failed", stage_name))?;
        current_fn(state)
            .await
            .with_context(|| format!("{} current phase failed", stage_name))?;
        Ok(())
    });
}

/// Bundles the shared state needed by the full (historical + live) pipeline.
///
/// Methods on this struct spawn collection/decoding stages into the provided
/// `JoinSet`, consuming the channels they need and cloning shared state.
struct FullPipelineContext {
    runtime: ChainRuntime,
    raw_config: Arc<RawDataCollectionConfig>,
    storage_manager: Arc<StorageManager>,
    catch_up_only: bool,
    live_mode_enabled: bool,
    repair: bool,
    repair_scope: Option<RepairScope>,
}

impl FullPipelineContext {
    fn chain(&self) -> &Arc<ChainConfig> {
        &self.runtime.chain
    }

    fn s3_manifest(&self) -> Option<storage::S3Manifest> {
        self.storage_manager.manifest_for(&self.chain().name)
    }

    /// Spawn the block collection stage.
    fn spawn_blocks(
        &self,
        tasks: &mut JoinSet<anyhow::Result<()>>,
        blocks_client: UnifiedRpcClient,
        block_tx: Option<mpsc::Sender<(u64, u64, Vec<alloy::primitives::B256>)>>,
        eth_call_tx: Option<mpsc::Sender<(u64, u64)>>,
    ) {
        let chain = self.chain().clone();
        let cfg = self.raw_config.clone();
        let s3_manifest = self.s3_manifest();
        let sm = self.storage_manager.clone();
        let catch_up_only = self.catch_up_only;

        tasks.spawn(async move {
            collect_blocks(
                &chain,
                &blocks_client,
                &cfg,
                block_tx,
                eth_call_tx,
                s3_manifest.as_ref(),
                Some(sm),
                catch_up_only,
            )
            .await
            .context("block collection failed")
        });
    }

    /// Spawn the receipt collection stage (catchup + current).
    #[allow(clippy::too_many_arguments)]
    fn spawn_receipts(
        &self,
        tasks: &mut JoinSet<anyhow::Result<()>>,
        receipts_client: UnifiedRpcClient,
        block_rx: mpsc::Receiver<(u64, u64, Vec<alloy::primitives::B256>)>,
        log_tx: mpsc::Sender<LogMessage>,
        factory_log_tx: Option<mpsc::Sender<LogMessage>>,
        event_trigger_tx: Option<mpsc::Sender<EventTriggerMessage>>,
        event_matchers: Vec<raw_data::historical::receipts::EventTriggerMatcher>,
        recollect_rx: Option<mpsc::Receiver<RecollectRequest>>,
    ) {
        let chain = self.chain().clone();
        let cfg = self.raw_config.clone();
        let s3_manifest = self.s3_manifest();
        let sm = self.storage_manager.clone();
        let log_tx = Some(log_tx);
        let receipts_client = Arc::new(receipts_client);

        spawn_two_phase_async(
            tasks,
            {
                let (chain, cfg, sm) = (chain.clone(), cfg.clone(), sm.clone());
                let receipts_client = receipts_client.clone();
                async move {
                    let sm_for_current = sm.clone();
                    raw_data::historical::catchup::receipts::collect_receipts(
                        &chain,
                        &receipts_client,
                        &cfg,
                        &log_tx,
                        &factory_log_tx,
                        &event_trigger_tx,
                        &event_matchers,
                        s3_manifest,
                        Some(sm),
                    )
                    .await
                    .context("receipt catchup failed")
                    .map(|state| {
                        (
                            state,
                            receipts_client,
                            chain,
                            cfg,
                            log_tx,
                            factory_log_tx,
                            event_trigger_tx,
                            event_matchers,
                            recollect_rx,
                            sm_for_current,
                        )
                    })
                }
            },
            |args| {
                let (
                    catchup_state,
                    receipts_client,
                    chain,
                    cfg,
                    log_tx,
                    factory_log_tx,
                    event_trigger_tx,
                    event_matchers,
                    recollect_rx,
                    sm,
                ) = args;
                async move {
                    raw_data::historical::current::receipts::collect_receipts(
                        &chain,
                        receipts_client,
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
            },
            "receipts",
        );
    }

    /// Spawn the log collection stage (catchup + current).
    fn spawn_logs(
        &self,
        tasks: &mut JoinSet<anyhow::Result<()>>,
        log_rx: mpsc::Receiver<LogMessage>,
        logs_factory_rx: Option<mpsc::Receiver<Arc<FactoryAddressData>>>,
        log_decoder_tx: Option<mpsc::Sender<DecoderMessage>>,
    ) {
        let chain = self.chain().clone();
        let cfg = self.raw_config.clone();
        let s3_manifest = self.s3_manifest();
        let sm = self.storage_manager.clone();

        spawn_two_phase_async(
            tasks,
            {
                let (chain, cfg) = (chain.clone(), cfg.clone());
                async move {
                    raw_data::historical::catchup::logs::collect_logs(
                        &chain,
                        &cfg,
                        s3_manifest,
                        Some(sm),
                    )
                    .await
                    .context("log catchup failed")
                }
            },
            move |catchup_state| async move {
                let max_pending = cfg
                    .max_pending_log_ranges
                    .unwrap_or(crate::types::config::defaults::raw_data::MAX_PENDING_LOG_RANGES);
                raw_data::historical::current::logs::collect_logs(
                    &chain,
                    log_rx,
                    logs_factory_rx,
                    log_decoder_tx,
                    catchup_state,
                    max_pending,
                )
                .await
                .context("log collection failed")
            },
            "logs",
        );
    }

    /// Spawn the eth_call collection stage (catchup + current).
    #[allow(clippy::too_many_arguments)]
    fn spawn_eth_calls(
        &self,
        tasks: &mut JoinSet<anyhow::Result<()>>,
        eth_calls_client: UnifiedRpcClient,
        eth_call_rx: mpsc::Receiver<(u64, u64)>,
        call_decoder_tx: Option<mpsc::Sender<DecoderMessage>>,
        eth_calls_factory_rx: Option<mpsc::Receiver<FactoryMessage>>,
        event_trigger_rx: Option<mpsc::Receiver<EventTriggerMessage>>,
        factory_catchup_done_rx: Option<oneshot::Receiver<()>>,
        eth_calls_catchup_done_tx: Option<oneshot::Sender<()>>,
    ) {
        let chain = self.chain().clone();
        let cfg = self.raw_config.clone();
        let s3_manifest = self.s3_manifest();
        let sm = self.storage_manager.clone();
        let has_factory_rx = eth_calls_factory_rx.is_some();
        let has_event_trigger_rx = event_trigger_rx.is_some();
        let repair = self.repair;
        let repair_scope = self.repair_scope.clone();

        spawn_two_phase_async(
            tasks,
            {
                let (chain, cfg, sm) = (chain.clone(), cfg.clone(), sm.clone());
                async move {
                    let sm_for_current = sm.clone();
                    raw_data::historical::catchup::eth_calls::collect_eth_calls(
                        &chain,
                        &eth_calls_client,
                        &cfg,
                        repair,
                        false,
                        repair_scope.clone(),
                        has_factory_rx,
                        has_event_trigger_rx,
                        factory_catchup_done_rx,
                        eth_calls_catchup_done_tx,
                        s3_manifest,
                        Some(sm),
                    )
                    .await
                    .context("eth_calls catchup failed")
                    .map(|state| {
                        (
                            state,
                            eth_calls_client,
                            chain,
                            cfg,
                            call_decoder_tx,
                            eth_calls_factory_rx,
                            event_trigger_rx,
                            sm_for_current,
                        )
                    })
                }
            },
            |args| {
                let (
                    catchup_state,
                    eth_calls_client,
                    chain,
                    cfg,
                    call_decoder_tx,
                    eth_calls_factory_rx,
                    event_trigger_rx,
                    sm,
                ) = args;
                let _ = cfg; // cfg not needed in current phase
                async move {
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
            },
            "eth_calls",
        );
    }

    /// Spawn the factory collection stage (catchup + current).
    #[allow(clippy::too_many_arguments)]
    fn spawn_factories(
        &self,
        tasks: &mut JoinSet<anyhow::Result<()>>,
        factory_log_rx: mpsc::Receiver<LogMessage>,
        logs_factory_tx: Option<mpsc::Sender<Arc<FactoryAddressData>>>,
        eth_calls_factory_tx: Option<mpsc::Sender<FactoryMessage>>,
        log_decoder_tx_for_factories: Option<mpsc::Sender<DecoderMessage>>,
        call_decoder_tx_for_factories: Option<mpsc::Sender<DecoderMessage>>,
        recollect_tx_for_factories: Option<mpsc::Sender<RecollectRequest>>,
        factory_catchup_done_tx: Option<oneshot::Sender<()>>,
    ) {
        let chain = self.chain().clone();
        let cfg = self.raw_config.clone();
        let s3_manifest = self.s3_manifest();
        let sm = self.storage_manager.clone();

        spawn_two_phase_async(
            tasks,
            {
                let (chain, cfg) = (chain.clone(), cfg.clone());
                async move {
                    raw_data::historical::catchup::factories::collect_factories(
                        &chain,
                        &cfg,
                        &logs_factory_tx,
                        &log_decoder_tx_for_factories,
                        &recollect_tx_for_factories,
                        factory_catchup_done_tx,
                        s3_manifest,
                        Some(sm),
                        false,
                    )
                    .await
                    .context("factory catchup failed")
                    .map(|state| {
                        (
                            state,
                            chain,
                            cfg,
                            logs_factory_tx,
                            log_decoder_tx_for_factories,
                            call_decoder_tx_for_factories,
                            eth_calls_factory_tx,
                        )
                    })
                }
            },
            |args| {
                let (
                    catchup_state,
                    chain,
                    cfg,
                    logs_factory_tx,
                    log_decoder_tx_for_factories,
                    call_decoder_tx_for_factories,
                    eth_calls_factory_tx,
                ) = args;
                async move {
                    raw_data::historical::current::factories::collect_factories(
                        &chain,
                        &cfg,
                        factory_log_rx,
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
            },
            "factories",
        );
    }

    /// Spawn the log decoder stage.
    fn spawn_log_decoder(
        &self,
        tasks: &mut JoinSet<anyhow::Result<()>>,
        log_decoder_rx: mpsc::Receiver<DecoderMessage>,
        transform_events_tx: Option<mpsc::Sender<transformations::DecodedEventsMessage>>,
        recollect_tx_for_log_decoder: Option<mpsc::Sender<RecollectRequest>>,
        transform_complete_tx: Option<mpsc::Sender<transformations::RangeCompleteMessage>>,
    ) {
        let chain = self.chain().clone();
        let cfg = self.raw_config.clone();

        tasks.spawn(async move {
            decode_logs(
                &chain,
                &cfg,
                log_decoder_rx,
                transform_events_tx,
                recollect_tx_for_log_decoder,
                transform_complete_tx,
                false,
            )
            .await
            .context("log decoding failed")
        });
    }

    /// Spawn the eth_call decoder stage.
    #[allow(clippy::too_many_arguments)]
    fn spawn_eth_call_decoder(
        &self,
        tasks: &mut JoinSet<anyhow::Result<()>>,
        call_decoder_rx: mpsc::Receiver<DecoderMessage>,
        transform_calls_tx: Option<mpsc::Sender<transformations::DecodedCallsMessage>>,
        transform_complete_tx: Option<mpsc::Sender<transformations::RangeCompleteMessage>>,
        transform_retry_tx: Option<mpsc::Sender<TransformRetryRequest>>,
        eth_calls_catchup_done_rx: Option<oneshot::Receiver<()>>,
        decode_catchup_done_tx: Option<oneshot::Sender<()>>,
    ) {
        let chain = self.chain().clone();
        let cfg = self.raw_config.clone();
        let repair = self.repair;
        let repair_scope = self.repair_scope.clone();

        tasks.spawn(async move {
            let outputs = decoding::EthCallDecoderOutputs {
                transform_tx: transform_calls_tx.as_ref(),
                complete_tx: transform_complete_tx.as_ref(),
                retry_tx: transform_retry_tx.as_ref(),
            };
            decode_eth_calls(
                &chain,
                &cfg,
                call_decoder_rx,
                outputs,
                eth_calls_catchup_done_rx,
                decode_catchup_done_tx,
                false,
                repair,
                repair_scope,
            )
            .await
            .context("eth call decoding failed")
        });
    }
}

#[allow(clippy::too_many_arguments)]
async fn process_chain(
    config: &IndexerConfig,
    chain: &ChainConfig,
    storage_manager: Arc<StorageManager>,
    catch_up_only: bool,
    repair: bool,
    repair_scope: Option<RepairScope>,
    shared_db_pool: Option<Arc<DbPool>>,
    shared_rate_limiter: Option<Arc<SlidingWindowRateLimiter>>,
    handler_filter: Option<&HashSet<String>>,
) -> anyhow::Result<()> {
    tracing::info!("Processing chain: {}", chain.name);

    // Build unified runtime (RPC client with rate limiter, db pool, registry, progress tracker)
    let runtime = ChainRuntime::build(
        config,
        chain,
        shared_db_pool,
        shared_rate_limiter,
        handler_filter,
    )
    .await?;

    let features = &runtime.features;
    let has_factories = features.has_factories;
    let has_events = features.has_events;
    let has_calls = features.has_calls;
    let has_factory_calls = features.has_factory_calls;
    let has_event_triggered_calls = features.has_event_triggered_calls;
    let needs_factory_filtering = features.needs_factory_filtering;
    let needs_recollect = features.needs_recollect;
    let transformations_enabled = runtime.transformations_enabled;

    // Build channels with config-derived capacity
    let CommonChannels {
        log_decoder_tx,
        log_decoder_rx,
        call_decoder_tx,
        call_decoder_rx,
        transform_events_tx,
        transform_events_rx,
        transform_calls_tx,
        transform_calls_rx,
        transform_complete_tx,
        transform_complete_rx,
        transform_reorg_tx,
        transform_reorg_rx,
        transform_retry_tx,
        transform_retry_rx,
        decode_catchup_done_tx,
        decode_catchup_done_rx,
    } = CommonChannels::build_for_full(config, features, transformations_enabled);

    // Historical-only channels
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
    let (logs_factory_tx, logs_factory_rx) =
        optional_channel::<Arc<FactoryAddressData>>(needs_factory_filtering, factory_cap);
    let (eth_calls_factory_tx, eth_calls_factory_rx) =
        optional_channel::<FactoryMessage>(has_factory_calls, factory_cap);
    let (event_trigger_tx, event_trigger_rx) =
        optional_channel::<EventTriggerMessage>(has_event_triggered_calls, channel_cap);
    let (recollect_tx, recollect_rx) =
        optional_channel::<RecollectRequest>(needs_recollect, channel_cap);

    // Catchup synchronization barriers
    let (factory_catchup_done_tx, factory_catchup_done_rx) = if has_factories {
        let (tx, rx) = oneshot::channel();
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    let (eth_calls_catchup_done_tx, eth_calls_catchup_done_rx) = if has_calls {
        let (tx, rx) = oneshot::channel();
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    let event_matchers = if has_event_triggered_calls {
        build_event_trigger_matchers(&runtime.chain.contracts)
    } else {
        Vec::new()
    };

    // Pre-create additional RPC clients sharing the runtime's rate limiter
    let blocks_client = runtime.build_additional_client()?;
    let receipts_client = runtime.build_additional_client()?;
    let eth_calls_client = runtime.build_additional_client()?;

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

    let live_mode_enabled = !catch_up_only
        && runtime.chain.to_block.is_none()
        && should_enable_live_mode(config, &runtime.chain);

    if !catch_up_only {
        if let Some(to_block) = runtime.chain.to_block {
            tracing::info!(
                "Live mode suppressed for chain {}: to_block={} is set",
                runtime.chain.name,
                to_block
            );
        }
    }

    // Clone for live mode transition (before originals are moved)
    let log_decoder_tx_for_live = if live_mode_enabled && has_events {
        log_decoder_tx.clone()
    } else {
        None
    };
    let eth_call_decoder_tx_for_live = if live_mode_enabled && has_calls {
        call_decoder_tx.clone()
    } else {
        None
    };

    // Build pipeline context
    let pipeline = FullPipelineContext {
        raw_config: runtime.raw_config(&config.raw_data_collection),
        runtime,
        storage_manager: storage_manager.clone(),
        catch_up_only,
        live_mode_enabled,
        repair,
        repair_scope,
    };

    let mut tasks: JoinSet<anyhow::Result<()>> = JoinSet::new();

    // In catch-up-only mode, drop the block and eth_call senders so the current
    // phases (which read from these channels) see closed channels and exit
    // immediately after catchup completes.
    let (block_tx_for_collector, eth_call_tx_for_collector) = if catch_up_only {
        drop(block_tx);
        drop(eth_call_tx);
        (None, None)
    } else {
        (Some(block_tx), Some(eth_call_tx))
    };

    // Spawn collection stages
    pipeline.spawn_blocks(
        &mut tasks,
        blocks_client,
        block_tx_for_collector,
        eth_call_tx_for_collector,
    );

    pipeline.spawn_receipts(
        &mut tasks,
        receipts_client,
        block_rx,
        log_tx,
        factory_log_tx,
        event_trigger_tx,
        event_matchers,
        recollect_rx,
    );

    pipeline.spawn_logs(&mut tasks, log_rx, logs_factory_rx, log_decoder_tx);

    pipeline.spawn_eth_calls(
        &mut tasks,
        eth_calls_client,
        eth_call_rx,
        call_decoder_tx,
        eth_calls_factory_rx,
        event_trigger_rx,
        factory_catchup_done_rx,
        eth_calls_catchup_done_tx,
    );

    if has_factories {
        pipeline.spawn_factories(
            &mut tasks,
            factory_log_rx.unwrap(),
            logs_factory_tx,
            eth_calls_factory_tx,
            log_decoder_tx_for_factories,
            call_decoder_tx_for_factories,
            recollect_tx_for_factories,
            factory_catchup_done_tx,
        );
    }

    // Spawn decoder stages
    if has_events {
        pipeline.spawn_log_decoder(
            &mut tasks,
            log_decoder_rx.unwrap(),
            transform_events_tx.clone(),
            recollect_tx_for_log_decoder,
            transform_complete_tx.clone(),
        );
    }

    if has_calls {
        pipeline.spawn_eth_call_decoder(
            &mut tasks,
            call_decoder_rx.unwrap(),
            transform_calls_tx.clone(),
            transform_complete_tx.clone(),
            transform_retry_tx.clone(),
            eth_calls_catchup_done_rx,
            decode_catchup_done_tx,
        );
    }

    // In catch-up-only mode, drop recollect_tx so the receipt current phase
    // (which waits for this channel to close) can exit after catchup data is processed.
    if catch_up_only {
        drop(recollect_tx);
    }

    // Create separate JoinSet for tasks that continue into live mode (transformation engine)
    let mut live_tasks: JoinSet<anyhow::Result<()>> = JoinSet::new();

    if transformations_enabled {
        let rpc_client = Arc::new(pipeline.runtime.build_additional_client()?);
        let tc = config.transformations.as_ref().unwrap();
        let mode = if tc.mode.batch_for_catchup {
            ExecutionMode::Batch {
                batch_size: tc.mode.catchup_batch_size,
            }
        } else {
            ExecutionMode::Streaming
        };

        let handler_count = pipeline.runtime.registry.handler_count();
        let retry_rx = transform_retry_rx;
        let engine = TransformationEngine::new(
            pipeline.runtime.registry.clone(),
            pipeline.runtime.db_pool.clone().unwrap(),
            rpc_client,
            TransformationEngineConfig {
                chain_name: pipeline.runtime.chain.name.clone(),
                chain_id: pipeline.runtime.chain.chain_id,
                mode,
                contracts: pipeline.runtime.chain.contracts.clone(),
                factory_collections: pipeline.runtime.chain.factory_collections.clone(),
                handler_concurrency: tc.handler_concurrency,
                expect_log_completion: has_events,
                expect_eth_call_completion: has_calls,
                from_block: pipeline.runtime.chain.from_block,
                to_block: pipeline.runtime.chain.to_block,
            },
            pipeline.runtime.progress_tracker.clone(),
        )
        .await
        .context("failed to create transformation engine")?;

        engine
            .initialize()
            .await
            .context("failed to initialize transformation handlers")?;

        tracing::info!(
            "Transformation engine initialized for chain {} with {} handlers",
            pipeline.runtime.chain.name,
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

    tracing::info!(
        "Completed historical processing for chain {}",
        pipeline.runtime.chain.name
    );

    // Transition to live mode (default behavior unless explicitly disabled)
    if catch_up_only {
        tracing::info!(
            "Catch-up-only mode complete for chain {}, skipping live mode",
            pipeline.runtime.chain.name
        );
        while let Some(result) = live_tasks.join_next().await {
            result.context("transformation engine task panicked")??;
        }
    } else if pipeline.live_mode_enabled {
        tracing::info!("Historical processing complete, transitioning to live mode");

        // Create HTTP client for live mode - shares rate limiter
        let http_client_for_live = Arc::new(pipeline.runtime.build_additional_client()?);

        spawn_live_mode(
            pipeline.runtime.chain.clone(),
            config,
            http_client_for_live,
            pipeline.runtime.db_pool.clone(),
            LiveModeChannels {
                log_decoder_tx: log_decoder_tx_for_live,
                eth_call_decoder_tx: eth_call_decoder_tx_for_live,
                transform_reorg_tx,
                transform_retry_tx,
            },
            pipeline.runtime.progress_tracker.clone(),
            Some(storage_manager.clone()),
            &mut live_tasks,
        )
        .await?;

        while let Some(result) = live_tasks.join_next().await {
            result.context("live mode task panicked")??;
        }
    } else {
        tracing::info!("Live mode not enabled, exiting after historical processing");
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

/// Channels for live mode communication with decoders/transformations.
struct LiveModeChannels {
    log_decoder_tx: Option<mpsc::Sender<DecoderMessage>>,
    eth_call_decoder_tx: Option<mpsc::Sender<DecoderMessage>>,
    transform_reorg_tx: Option<mpsc::Sender<ReorgMessage>>,
    transform_retry_tx: Option<mpsc::Sender<TransformRetryRequest>>,
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
#[allow(clippy::too_many_arguments)]
async fn spawn_live_mode(
    chain: Arc<ChainConfig>,
    config: &IndexerConfig,
    http_client: Arc<UnifiedRpcClient>,
    db_pool: Option<Arc<DbPool>>,
    live_channels: LiveModeChannels,
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
    let eth_call_collector = if live_channels.eth_call_decoder_tx.is_some() {
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
        live_channels.log_decoder_tx.is_some(),
        eth_call_collector.is_some(),
        live_channels.eth_call_decoder_tx.is_some() && eth_call_collector.is_some(),
        live_channels.transform_retry_tx.is_some(),
    );

    // Start live collector
    let collector = LiveCollector::new(LiveCollectorConfig {
        chain: chain.clone(),
        http_client: http_client.clone(),
        config: live_config.clone(),
        progress_tracker: Some(progress_tracker.clone()),
        factory_matchers,
        eth_call_collector,
        db_pool: db_pool.clone(),
        expectations: live_expectations,
        transform_retry_tx: live_channels.transform_retry_tx.clone(),
    });
    tasks.spawn(async move {
        collector
            .run(
                ws_event_rx,
                live_msg_tx,
                live_channels.log_decoder_tx,
                live_channels.eth_call_decoder_tx,
                live_channels.transform_reorg_tx,
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
    if let Some(retry_tx) = live_channels.transform_retry_tx {
        compaction = compaction.with_retry_tx(retry_tx);
    }
    tasks.spawn(async move {
        compaction.run().await;
        Ok(())
    });

    Ok(())
}
