//! Chain runtime infrastructure setup.
//!
//! This module provides unified setup for both full (historical + live) and
//! live-only modes, ensuring consistent configuration and avoiding duplication.

use std::future::Future;
use std::sync::Arc;

use anyhow::Context;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinSet;

use crate::cli::IndexerMode;
use crate::db::DbPool;
use crate::decoding::{self, decode_eth_calls, decode_logs, DecoderMessage};
use crate::live::{LiveProgressTracker, TransformRetryRequest};
use crate::raw_data::historical::catchup::blocks::collect_blocks;
use crate::raw_data::historical::factories::{FactoryAddressData, FactoryMessage, RecollectRequest};
use crate::raw_data::historical::receipts::{EventTriggerMessage, LogMessage};
use crate::rpc::{SlidingWindowRateLimiter, UnifiedRpcClient};
use crate::storage::{self, StorageManager};
use crate::transformations::registry::TransformationRegistry;
use crate::transformations::{
    build_registry_for_chain, DecodedCallsMessage, DecodedEventsMessage, RangeCompleteMessage,
    ReorgMessage,
};
use crate::types::config::chain::{ChainConfig, RpcConfig};
use crate::types::config::defaults::{raw_data as raw_data_defaults, rpc as rpc_defaults};
use crate::types::config::eth_call::Frequency;
use crate::types::config::indexer::IndexerConfig;
use crate::types::config::raw_data::RawDataCollectionConfig;
use crate::types::shared::repair::RepairScope;
use crate::{has_items, optional_channel};

/// Feature flags derived from chain configuration.
///
/// Centralizes the feature detection logic that determines which
/// pipeline components need to be activated.
#[derive(Debug, Clone)]
pub struct ChainFeatures {
    pub has_factories: bool,
    pub has_events: bool,
    pub has_calls: bool,
    pub has_factory_calls: bool,
    pub has_event_triggered_calls: bool,
    pub contract_logs_only: bool,
    pub needs_factory_filtering: bool,
    pub needs_recollect: bool,
}

impl ChainFeatures {
    /// Detect features from chain configuration.
    pub fn detect(chain: &ChainConfig, config: &IndexerConfig) -> Self {
        let has_factories = chain.contracts.values().any(|c| has_items(&c.factories));

        let contract_logs_only = config
            .raw_data_collection
            .contract_logs_only
            .unwrap_or(false);

        let needs_factory_filtering = has_factories && contract_logs_only;

        let has_factory_calls = chain.contracts.values().any(|c| {
            c.factories
                .as_ref()
                .is_some_and(|factories| factories.iter().any(|f| has_items(&f.calls)))
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

        let needs_recollect = has_factories || has_events;

        Self {
            has_factories,
            has_events,
            has_calls,
            has_factory_calls,
            has_event_triggered_calls,
            contract_logs_only,
            needs_factory_filtering,
            needs_recollect,
        }
    }

    pub fn log_summary(&self, chain_name: &str) {
        tracing::info!(
            "Chain {} - has_factories: {}, contract_logs_only: {}, has_factory_calls: {}, has_event_triggered_calls: {}",
            chain_name,
            self.has_factories,
            self.contract_logs_only,
            self.has_factory_calls,
            self.has_event_triggered_calls
        );
        tracing::info!(
            "Chain {} - decode_logs: {}, decode_calls: {}",
            chain_name,
            self.has_events,
            self.has_calls
        );
    }
}

/// Build RPC client with rate limiter from per-chain config.
///
/// Returns both the shared rate limiter and the client, so additional
/// clients can share the same limiter for account-level rate limiting.
pub fn build_rpc_client(
    rpc_url: &str,
    rpc_config: &RpcConfig,
    batch_size: usize,
) -> anyhow::Result<(Arc<SlidingWindowRateLimiter>, Arc<UnifiedRpcClient>)> {
    let concurrency = rpc_config.concurrency.unwrap_or(rpc_defaults::CONCURRENCY);
    let cu_per_second = rpc_config
        .compute_units_per_second
        .unwrap_or(rpc_defaults::ALCHEMY_CU_PER_SECOND);

    let rate_limiter = Arc::new(SlidingWindowRateLimiter::new(cu_per_second));

    let client = UnifiedRpcClient::from_url_with_options(
        rpc_url,
        cu_per_second,
        concurrency,
        batch_size,
        Some(rate_limiter.clone()),
    )?;

    tracing::info!(
        "RPC config: concurrency={}, cu_per_second={}, batch_size={}",
        concurrency,
        cu_per_second,
        batch_size
    );

    Ok((rate_limiter, Arc::new(client)))
}

/// Build an additional RPC client sharing the same rate limiter.
pub fn build_rpc_client_with_limiter(
    rpc_url: &str,
    rpc_config: &RpcConfig,
    batch_size: usize,
    shared_limiter: Arc<SlidingWindowRateLimiter>,
) -> anyhow::Result<UnifiedRpcClient> {
    let concurrency = rpc_config.concurrency.unwrap_or(rpc_defaults::CONCURRENCY);
    let cu_per_second = rpc_config
        .compute_units_per_second
        .unwrap_or(rpc_defaults::ALCHEMY_CU_PER_SECOND);

    UnifiedRpcClient::from_url_with_options(
        rpc_url,
        cu_per_second,
        concurrency,
        batch_size,
        Some(shared_limiter),
    )
    .map_err(Into::into)
}

/// Channels used by both modes for decoders and transformations.
pub struct CommonChannels {
    // Decoder channels
    pub log_decoder_tx: Option<mpsc::Sender<DecoderMessage>>,
    pub log_decoder_rx: Option<mpsc::Receiver<DecoderMessage>>,
    pub call_decoder_tx: Option<mpsc::Sender<DecoderMessage>>,
    pub call_decoder_rx: Option<mpsc::Receiver<DecoderMessage>>,

    // Transform channels
    pub transform_events_tx: Option<mpsc::Sender<DecodedEventsMessage>>,
    pub transform_events_rx: Option<mpsc::Receiver<DecodedEventsMessage>>,
    pub transform_calls_tx: Option<mpsc::Sender<DecodedCallsMessage>>,
    pub transform_calls_rx: Option<mpsc::Receiver<DecodedCallsMessage>>,
    pub transform_complete_tx: Option<mpsc::Sender<RangeCompleteMessage>>,
    pub transform_complete_rx: Option<mpsc::Receiver<RangeCompleteMessage>>,
    pub transform_reorg_tx: Option<mpsc::Sender<ReorgMessage>>,
    pub transform_reorg_rx: Option<mpsc::Receiver<ReorgMessage>>,
    pub transform_retry_tx: Option<mpsc::Sender<TransformRetryRequest>>,
    pub transform_retry_rx: Option<mpsc::Receiver<TransformRetryRequest>>,

    // Decode catchup barrier (full mode only, None for live-only)
    pub decode_catchup_done_tx: Option<oneshot::Sender<()>>,
    pub decode_catchup_done_rx: Option<oneshot::Receiver<()>>,
}

impl CommonChannels {
    /// Build channels for live-only mode.
    ///
    /// Creates decoder and transform channels with config-derived capacity.
    /// Historical-only channels (barriers, etc.) are None.
    pub fn build_for_live_only(
        config: &IndexerConfig,
        features: &ChainFeatures,
        transformations_enabled: bool,
    ) -> Self {
        let channel_cap = config
            .raw_data_collection
            .channel_capacity
            .unwrap_or(raw_data_defaults::CHANNEL_CAPACITY);

        // Decoder channels
        let (log_decoder_tx, log_decoder_rx) = if features.has_events {
            let (tx, rx) = mpsc::channel(channel_cap);
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        let (call_decoder_tx, call_decoder_rx) = if features.has_calls {
            let (tx, rx) = mpsc::channel(channel_cap);
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        // Transform channels
        let (transform_events_tx, transform_events_rx) =
            optional_channel::<DecodedEventsMessage>(transformations_enabled, channel_cap);
        let (transform_calls_tx, transform_calls_rx) =
            optional_channel::<DecodedCallsMessage>(transformations_enabled, channel_cap);
        let (transform_complete_tx, transform_complete_rx) =
            optional_channel::<RangeCompleteMessage>(transformations_enabled, channel_cap);
        let (transform_reorg_tx, transform_reorg_rx) =
            optional_channel::<ReorgMessage>(transformations_enabled, channel_cap);
        let (transform_retry_tx, transform_retry_rx) =
            optional_channel::<TransformRetryRequest>(transformations_enabled, 100);

        Self {
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
            decode_catchup_done_tx: None,
            decode_catchup_done_rx: None,
        }
    }

    /// Build channels for full mode.
    ///
    /// Same as live-only but also creates the decode catchup barrier
    /// if transformations and calls are both enabled.
    pub fn build_for_full(
        config: &IndexerConfig,
        features: &ChainFeatures,
        transformations_enabled: bool,
    ) -> Self {
        let mut channels = Self::build_for_live_only(config, features, transformations_enabled);

        // Add decode catchup barrier for full mode
        if transformations_enabled && features.has_calls {
            let (tx, rx) = oneshot::channel();
            channels.decode_catchup_done_tx = Some(tx);
            channels.decode_catchup_done_rx = Some(rx);
        }

        channels
    }
}

/// Shared runtime infrastructure for a chain.
///
/// Encapsulates the common setup needed by both full and live-only modes:
/// - Feature detection
/// - RPC client with rate limiting
/// - Database pool (if transformations enabled)
/// - Transformation registry
/// - Progress tracker (if transformations enabled)
pub struct ChainRuntime {
    pub features: ChainFeatures,
    pub rate_limiter: Arc<SlidingWindowRateLimiter>,
    pub http_client: Arc<UnifiedRpcClient>,
    pub rpc_url: String,
    pub rpc_batch_size: usize,
    pub db_pool: Option<Arc<DbPool>>,
    pub registry: Arc<TransformationRegistry>,
    pub progress_tracker: Option<Arc<Mutex<LiveProgressTracker>>>,
    pub transformations_enabled: bool,
    pub chain: Arc<ChainConfig>,
}

impl ChainRuntime {
    /// Build the runtime infrastructure for a chain.
    ///
    /// When running multiple chains concurrently, pass a pre-created `shared_db_pool`
    /// (with migrations already run) and `shared_rate_limiter` to avoid redundant
    /// setup and enable account-level rate limiting across chains.
    pub async fn build(
        config: &IndexerConfig,
        chain: &ChainConfig,
        shared_db_pool: Option<Arc<DbPool>>,
        shared_rate_limiter: Option<Arc<SlidingWindowRateLimiter>>,
    ) -> anyhow::Result<Self> {
        let rpc_url = std::env::var(&chain.rpc_url_env_var).with_context(|| {
            format!(
                "env var {} not set for chain {}",
                chain.rpc_url_env_var, chain.name
            )
        })?;

        // Feature detection
        let features = ChainFeatures::detect(chain, config);
        features.log_summary(&chain.name);

        // Resolve batch size with full fallback chain:
        // chain.rpc.batch_size -> raw_data_collection.rpc_batch_size -> default
        let rpc_batch_size = chain
            .rpc
            .batch_size
            .or(config.raw_data_collection.rpc_batch_size)
            .unwrap_or(rpc_defaults::MAX_BATCH_SIZE) as usize;

        // Resolve RPC parameters before building client
        let concurrency = chain.rpc.concurrency.unwrap_or(rpc_defaults::CONCURRENCY);
        let cu_per_second = chain
            .rpc
            .compute_units_per_second
            .unwrap_or(rpc_defaults::ALCHEMY_CU_PER_SECOND);

        // Build RPC client, reusing shared rate limiter if provided
        let (rate_limiter, http_client) = if let Some(limiter) = shared_rate_limiter {
            let client = UnifiedRpcClient::from_url_with_options(
                &rpc_url,
                cu_per_second,
                concurrency,
                rpc_batch_size,
                Some(limiter.clone()),
            )?;
            (limiter, Arc::new(client))
        } else {
            build_rpc_client(&rpc_url, &chain.rpc, rpc_batch_size)?
        };

        // Build transformation registry filtered to this chain's contracts
        let registry =
            build_registry_for_chain(chain.chain_id, &chain.contracts, &chain.factory_collections);
        let transformations_enabled = config.transformations.is_some() && !registry.is_empty();

        // Validate that all handler call dependencies are satisfied by config
        if transformations_enabled {
            crate::transformations::registry::validate_call_dependencies(
                &registry,
                &chain.contracts,
                &chain.factory_collections,
            );
        }

        // Setup database if transformations enabled
        let db_pool = if transformations_enabled {
            if let Some(pool) = shared_db_pool {
                // Use pre-created shared pool (migrations already run in main)
                Some(pool)
            } else {
                // Fallback: create per-chain pool (for backwards compat or single-chain mode)
                let tc = config.transformations.as_ref().unwrap();
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

                tracing::info!("Database pool initialized and migrations complete");
                Some(Arc::new(pool))
            }
        } else {
            None
        };

        // Create progress tracker if transformations enabled
        let progress_tracker = if transformations_enabled {
            let tracker = Arc::new(Mutex::new(LiveProgressTracker::new(
                chain.chain_id as i64,
                db_pool.clone(),
                chain.name.clone(),
            )));

            // Register all handlers
            {
                let mut t = tracker.lock().await;
                for handler in registry.all_handlers() {
                    t.register_handler(&handler.handler_key());
                }
            }
            tracing::info!(
                "Registered {} handlers with live progress tracker",
                registry.all_handlers().len()
            );

            Some(tracker)
        } else {
            None
        };

        Ok(Self {
            features,
            rate_limiter,
            http_client,
            rpc_url,
            rpc_batch_size,
            db_pool,
            registry: Arc::new(registry),
            progress_tracker,
            transformations_enabled,
            chain: Arc::new(chain.clone()),
        })
    }

    /// Create an additional RPC client sharing the same rate limiter.
    pub fn build_additional_client(&self) -> anyhow::Result<UnifiedRpcClient> {
        build_rpc_client_with_limiter(
            &self.rpc_url,
            &self.chain.rpc,
            self.rpc_batch_size,
            self.rate_limiter.clone(),
        )
    }

    /// Build a `RawDataCollectionConfig` with the resolved batch size applied.
    ///
    /// Collectors use `raw_config.rpc_batch_size` to determine batch sizes,
    /// so we patch the config with the runtime's resolved value.
    pub fn raw_config(&self, base: &RawDataCollectionConfig) -> Arc<RawDataCollectionConfig> {
        let mut cfg = base.clone();
        cfg.rpc_batch_size = Some(self.rpc_batch_size as u32);
        Arc::new(cfg)
    }
}

// ---------------------------------------------------------------------------
// Full pipeline context
// ---------------------------------------------------------------------------

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
pub fn spawn_two_phase_async<S, CatchupFut, CurrentFut, CurrentFn>(
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
pub struct FullPipelineContext {
    pub runtime: ChainRuntime,
    pub raw_config: Arc<RawDataCollectionConfig>,
    pub storage_manager: Arc<StorageManager>,
    pub mode: IndexerMode,
    pub live_mode_enabled: bool,
}

impl FullPipelineContext {
    pub fn chain(&self) -> &Arc<ChainConfig> {
        &self.runtime.chain
    }

    pub fn s3_manifest(&self) -> Option<storage::S3Manifest> {
        self.storage_manager.manifest_for(&self.chain().name)
    }

    /// Spawn the block collection stage.
    pub fn spawn_blocks(
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
        let catch_up_only = self.mode.is_catch_up_only();

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
    pub fn spawn_receipts(
        &self,
        tasks: &mut JoinSet<anyhow::Result<()>>,
        receipts_client: UnifiedRpcClient,
        block_rx: mpsc::Receiver<(u64, u64, Vec<alloy::primitives::B256>)>,
        log_tx: mpsc::Sender<LogMessage>,
        factory_log_tx: Option<mpsc::Sender<LogMessage>>,
        event_trigger_tx: Option<mpsc::Sender<EventTriggerMessage>>,
        event_matchers: Vec<crate::raw_data::historical::receipts::EventTriggerMatcher>,
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
                    crate::raw_data::historical::catchup::receipts::collect_receipts(
                        &chain,
                        &*receipts_client,
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
                    crate::raw_data::historical::current::receipts::collect_receipts(
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
    pub fn spawn_logs(
        &self,
        tasks: &mut JoinSet<anyhow::Result<()>>,
        log_rx: mpsc::Receiver<LogMessage>,
        logs_factory_rx: Option<mpsc::Receiver<FactoryAddressData>>,
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
                    crate::raw_data::historical::catchup::logs::collect_logs(
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
                crate::raw_data::historical::current::logs::collect_logs(
                    &chain,
                    log_rx,
                    logs_factory_rx,
                    log_decoder_tx,
                    catchup_state,
                )
                .await
                .context("log collection failed")
            },
            "logs",
        );
    }

    /// Spawn the eth_call collection stage (catchup + current).
    #[allow(clippy::too_many_arguments)]
    pub fn spawn_eth_calls(
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
        let repair = self.mode.is_repair();
        let repair_scope = self.mode.repair_scope().cloned();

        spawn_two_phase_async(
            tasks,
            {
                let (chain, cfg, sm) = (chain.clone(), cfg.clone(), sm.clone());
                async move {
                    let sm_for_current = sm.clone();
                    crate::raw_data::historical::catchup::eth_calls::collect_eth_calls(
                        &chain,
                        &eth_calls_client,
                        &cfg,
                        repair,
                        false,
                        repair_scope.clone(),
                        &call_decoder_tx,
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
                    crate::raw_data::historical::current::eth_calls::collect_eth_calls(
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
    pub fn spawn_factories(
        &self,
        tasks: &mut JoinSet<anyhow::Result<()>>,
        factory_log_rx: mpsc::Receiver<LogMessage>,
        logs_factory_tx: Option<mpsc::Sender<FactoryAddressData>>,
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
                    crate::raw_data::historical::catchup::factories::collect_factories(
                        &chain,
                        &cfg,
                        &logs_factory_tx,
                        &log_decoder_tx_for_factories,
                        &recollect_tx_for_factories,
                        factory_catchup_done_tx,
                        s3_manifest,
                        Some(sm),
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
                    crate::raw_data::historical::current::factories::collect_factories(
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
    pub fn spawn_log_decoder(
        &self,
        tasks: &mut JoinSet<anyhow::Result<()>>,
        log_decoder_rx: mpsc::Receiver<DecoderMessage>,
        transform_events_tx: Option<mpsc::Sender<DecodedEventsMessage>>,
        recollect_tx_for_log_decoder: Option<mpsc::Sender<RecollectRequest>>,
        transform_complete_tx: Option<mpsc::Sender<RangeCompleteMessage>>,
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
    pub fn spawn_eth_call_decoder(
        &self,
        tasks: &mut JoinSet<anyhow::Result<()>>,
        call_decoder_rx: mpsc::Receiver<DecoderMessage>,
        transform_calls_tx: Option<mpsc::Sender<DecodedCallsMessage>>,
        transform_complete_tx: Option<mpsc::Sender<RangeCompleteMessage>>,
        transform_retry_tx: Option<mpsc::Sender<TransformRetryRequest>>,
        eth_calls_catchup_done_rx: Option<oneshot::Receiver<()>>,
        decode_catchup_done_tx: Option<oneshot::Sender<()>>,
    ) {
        let chain = self.chain().clone();
        let cfg = self.raw_config.clone();
        let repair = self.mode.is_repair();
        let repair_scope = self.mode.repair_scope().cloned();

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
