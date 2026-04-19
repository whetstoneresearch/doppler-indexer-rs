//! Solana pipeline orchestration.
//!
//! Wires together raw data collection, decoding, and transformation for Solana
//! chains. Parallel to the EVM pipeline functions in `main.rs`.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use anyhow::Context;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinSet;

use crate::db::DbPool;
use crate::decoding::DecoderMessage;
use crate::live::LiveModeConfig;
use crate::live::LiveProgressTracker;
use crate::solana::decoding::accounts::SolanaAccountDecoder;
use crate::solana::decoding::events::SolanaEventDecoder;
use crate::solana::decoding::idl::AnchorDecoder;
use crate::solana::decoding::instructions::SolanaInstructionDecoder;
use crate::solana::decoding::spl_token::SplTokenDecoder;
use crate::solana::decoding::tasks::{decode_solana_events, decode_solana_instructions};
use crate::solana::decoding::traits::ProgramDecoder;
use crate::solana::discovery::{DiscoveryEventData, DiscoveryManager, SharedKnownAccounts};
use crate::solana::live::accounts::SolanaLiveAccountReader;
use crate::solana::live::catchup::{SolanaCatchupScanResult, SolanaLiveCatchupService};
use crate::solana::live::collector::SolanaLiveCollector;
use crate::solana::live::compaction::SolanaCompactionService;
use crate::solana::live::reorg::SolanaReorgDetector;
use crate::solana::live::storage::SolanaLiveStorage;
use crate::solana::live::types::{AccountReadTrigger, SolanaLivePipelineExpectations};
use crate::solana::raw_data::catchup::signature_driven_backfill;
use crate::solana::raw_data::parquet::{read_events_from_parquet, read_instructions_from_parquet};
use crate::solana::rpc::SolanaRpcClient;
use crate::solana::ws::SolanaWsClient;
use crate::storage::paths::{
    raw_solana_events_dir, raw_solana_instructions_dir, scan_parquet_ranges,
};
use crate::transformations::engine::{
    DecodedEventsMessage, ExecutionMode, ReorgMessage, TransformationEngine,
    TransformationEngineConfig,
};
use crate::transformations::registry::{
    build_registry_for_solana_chain, extract_event_name,
    validate_account_state_dependencies_for_solana, TransformationRegistry,
};
use crate::types::config::chain::ChainConfig;
use crate::types::config::contract::{Contracts, FactoryCollections};
use crate::types::config::defaults;
use crate::types::config::indexer::IndexerConfig;
use crate::types::config::solana::{SolanaCommitment, SolanaPrograms};
use crate::types::shared::repair::RepairScope;

// ---------------------------------------------------------------------------
// Feature detection
// ---------------------------------------------------------------------------

/// Feature flags derived from a Solana chain's program configuration.
#[derive(Debug, Clone)]
pub struct SolanaChainFeatures {
    pub has_events: bool,
    pub has_instructions: bool,
    pub has_discovery: bool,
    pub has_on_events_account_reads: bool,
    pub has_account_reads: bool,
}

impl SolanaChainFeatures {
    pub fn detect(programs: &SolanaPrograms) -> Self {
        let mut has_events = false;
        let mut has_instructions = false;
        let mut has_discovery = false;
        let mut has_on_events_account_reads = false;
        let mut has_account_reads = false;

        for program in programs.values() {
            // Events: program has an IDL, explicit event config, or a built-in decoder
            if program.idl_path.is_some()
                || program.decoder.is_some()
                || program.events.as_ref().is_some_and(|e| !e.is_empty())
            {
                has_events = true;
            }

            // Instructions: program has an IDL or built-in decoder
            if program.idl_path.is_some() || program.decoder.is_some() {
                has_instructions = true;
            }

            if program.discovery.as_ref().is_some_and(|d| !d.is_empty()) {
                has_discovery = true;
            }

            if program.accounts.as_ref().is_some_and(|a| !a.is_empty()) {
                has_account_reads = true;
                if program.accounts.as_ref().is_some_and(|accounts| {
                    accounts
                        .iter()
                        .any(|account| account.frequency.is_on_events())
                }) {
                    has_on_events_account_reads = true;
                }
            }
        }

        Self {
            has_events,
            has_instructions,
            has_discovery,
            has_on_events_account_reads,
            has_account_reads,
        }
    }
}

// ---------------------------------------------------------------------------
// Runtime
// ---------------------------------------------------------------------------

/// Runtime infrastructure for a Solana chain.
///
/// Parallel to the EVM [`ChainRuntime`](crate::runtime::ChainRuntime) with
/// Solana-specific fields.
pub struct SolanaChainRuntime {
    pub rpc_client: Arc<SolanaRpcClient>,
    pub chain: Arc<ChainConfig>,
    pub db_pool: Option<Arc<DbPool>>,
    pub registry: Arc<TransformationRegistry>,
    pub decoders: Vec<Arc<dyn ProgramDecoder>>,
    pub configured_programs: HashSet<[u8; 32]>,
    pub program_names: Arc<HashMap<[u8; 32], String>>,
    pub features: SolanaChainFeatures,
    pub transformations_enabled: bool,
    pub progress_tracker: Option<Arc<Mutex<LiveProgressTracker>>>,
}

impl SolanaChainRuntime {
    pub async fn build(
        config: &IndexerConfig,
        chain: &ChainConfig,
        shared_db_pool: Option<Arc<DbPool>>,
        handler_filter: Option<&HashSet<String>>,
    ) -> anyhow::Result<Self> {
        let chain = Arc::new(chain.clone());

        // Build RPC client
        let rpc_url = std::env::var(&chain.rpc_url_env_var)
            .with_context(|| format!("env var {} not set for Solana RPC", chain.rpc_url_env_var))?;
        let rpc_client = Arc::new(SolanaRpcClient::new(
            &rpc_url,
            chain.commitment,
            chain.rpc.units_per_second(),
            chain.rpc.concurrency,
        )?);

        // Detect features
        let features = SolanaChainFeatures::detect(&chain.solana_programs);

        // Build decoders
        let decoders = build_decoders(&chain)?;

        // Build program_id -> name mapping and configured program set
        let program_names = Arc::new(build_program_names(&chain)?);
        let configured_programs = build_configured_programs(&chain)?;

        // Build registry
        let mut registry = build_registry_for_solana_chain(chain.chain_id, &chain.solana_programs);
        if let Some(names) = handler_filter {
            registry.filter_to_handlers(names);
        }
        let registry = Arc::new(registry);

        // Transformations enabled if registry has handlers AND db config present
        let transformations_enabled =
            registry.handler_count() > 0 && config.transformations.is_some();

        if transformations_enabled {
            validate_account_state_dependencies_for_solana(&registry, &chain.solana_programs);
        }

        // Setup database if transformations enabled
        let db_pool = if transformations_enabled {
            if let Some(pool) = shared_db_pool {
                Some(pool)
            } else {
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

                tracing::info!("Solana database pool initialized and migrations complete");
                Some(Arc::new(pool))
            }
        } else {
            None
        };

        // Create progress tracker with Solana status storage backend
        // so handler completion flags are written to LiveSlotStatus files
        // instead of the EVM LiveBlockStatus default.
        let progress_tracker = if transformations_enabled {
            let tracker = Arc::new(Mutex::new(LiveProgressTracker::new(
                chain.chain_id as i64,
                db_pool.clone(),
                chain.name.clone(),
            )));
            {
                let mut t = tracker.lock().await;
                t.set_status_storage(Arc::new(
                    crate::solana::live::storage::SolanaLiveStorage::new(&chain.name),
                ));
                for handler in registry.all_handlers() {
                    t.register_handler(&handler.handler_key());
                }
            }
            Some(tracker)
        } else {
            None
        };

        tracing::info!(
            chain = chain.name.as_str(),
            has_events = features.has_events,
            has_instructions = features.has_instructions,
            has_discovery = features.has_discovery,
            has_on_events_account_reads = features.has_on_events_account_reads,
            has_account_reads = features.has_account_reads,
            transformations_enabled,
            programs = chain.solana_programs.len(),
            decoders = decoders.len(),
            handlers = registry.handler_count(),
            "Solana chain runtime built"
        );

        Ok(Self {
            rpc_client,
            chain,
            db_pool,
            registry,
            decoders,
            configured_programs,
            program_names,
            features,
            transformations_enabled,
            progress_tracker,
        })
    }
}

// ---------------------------------------------------------------------------
// Decoder construction
// ---------------------------------------------------------------------------

/// Build [`ProgramDecoder`] instances from the chain's Solana program configs.
fn required_decoder_capabilities(
    program: &crate::types::config::solana::SolanaProgramConfig,
) -> Vec<&'static str> {
    let mut capabilities = Vec::new();

    if program
        .events
        .as_ref()
        .is_some_and(|events| !events.is_empty())
    {
        capabilities.push("events");
    }
    if program
        .accounts
        .as_ref()
        .is_some_and(|accounts| !accounts.is_empty())
    {
        capabilities.push("accounts");
    }
    if program
        .discovery
        .as_ref()
        .is_some_and(|discovery| !discovery.is_empty())
    {
        capabilities.push("discovery");
    }

    capabilities
}

/// Build [`ProgramDecoder`] instances from the chain's Solana program configs.
fn build_decoders(chain: &ChainConfig) -> anyhow::Result<Vec<Arc<dyn ProgramDecoder>>> {
    let mut decoders: Vec<Arc<dyn ProgramDecoder>> = Vec::new();

    for (name, program) in &chain.solana_programs {
        let program_id_bytes = parse_program_id(&program.program_id)
            .with_context(|| format!("invalid program_id for '{}'", name))?;

        // Built-in decoder takes precedence
        if let Some(ref decoder_name) = program.decoder {
            match decoder_name.as_str() {
                "spl_token" => {
                    tracing::info!(program = name.as_str(), "Using built-in SPL Token decoder");
                    decoders.push(Arc::new(SplTokenDecoder::new()));
                }
                other => {
                    anyhow::bail!(
                        "Unknown built-in decoder '{}' configured for Solana program '{}'",
                        other,
                        name
                    );
                }
            }
            continue;
        }

        // IDL-based decoder
        if let Some(ref idl_path) = program.idl_path {
            let idl_json = std::fs::read_to_string(idl_path)
                .with_context(|| format!("failed to read IDL for '{}' at {}", name, idl_path))?;

            let anchor = AnchorDecoder::new(program_id_bytes, name.clone(), &idl_json)
                .map_err(|e| anyhow::anyhow!("failed to parse IDL for '{}': {}", name, e))?;

            tracing::info!(
                program = name.as_str(),
                idl_path,
                events = anchor.event_types().len(),
                instructions = anchor.instruction_types().len(),
                accounts = anchor.account_types().len(),
                "Loaded Anchor IDL decoder"
            );
            decoders.push(Arc::new(anchor));
            continue;
        }

        let required_capabilities = required_decoder_capabilities(program);
        if !required_capabilities.is_empty() {
            anyhow::bail!(
                "Solana program '{}' enables {} but has no decoder or IDL configured",
                name,
                required_capabilities.join(", ")
            );
        }

        tracing::debug!(
            program = name.as_str(),
            "No decoder configured; skipping program without decode-dependent stages"
        );
    }

    Ok(decoders)
}

/// Build `program_id bytes -> config key name` mapping.
fn build_program_names(chain: &ChainConfig) -> anyhow::Result<HashMap<[u8; 32], String>> {
    let mut names = HashMap::new();
    for (name, program) in &chain.solana_programs {
        let bytes = parse_program_id(&program.program_id)
            .with_context(|| format!("invalid program_id for '{}'", name))?;
        names.insert(bytes, name.clone());
    }
    Ok(names)
}

/// Build the set of configured program IDs (as bytes) for raw data filtering.
fn build_configured_programs(chain: &ChainConfig) -> anyhow::Result<HashSet<[u8; 32]>> {
    let mut programs = HashSet::new();
    for (name, program) in &chain.solana_programs {
        let bytes = parse_program_id(&program.program_id)
            .with_context(|| format!("invalid program_id for '{}'", name))?;
        programs.insert(bytes);
    }
    Ok(programs)
}

/// Decode a base58 program ID string into 32 bytes.
fn parse_program_id(program_id: &str) -> anyhow::Result<[u8; 32]> {
    let decoded = bs58::decode(program_id)
        .into_vec()
        .map_err(|e| anyhow::anyhow!("invalid base58: {}", e))?;
    let bytes: [u8; 32] = decoded
        .try_into()
        .map_err(|v: Vec<u8>| anyhow::anyhow!("expected 32 bytes, got {}", v.len()))?;
    Ok(bytes)
}

// ---------------------------------------------------------------------------
// Historical pipeline
// ---------------------------------------------------------------------------

/// Run the Solana historical pipeline for a chain.
///
/// 1. Build runtime (RPC client, decoders, registry)
/// 2. Create channels between collection → decoding → transformation
/// 3. Spawn signature-driven backfill
/// 4. Spawn decoder tasks
/// 5. (Future) Spawn transformation engine when handlers exist
/// 6. Wait for completion
/// 7. Stub live mode
pub async fn process_solana_chain(
    config: &IndexerConfig,
    chain: &ChainConfig,
    catch_up_only: bool,
    repair: bool,
    repair_scope: Option<RepairScope>,
    shared_db_pool: Option<Arc<DbPool>>,
    handler_filter: Option<&HashSet<String>>,
) -> anyhow::Result<()> {
    tracing::info!(
        chain = chain.name.as_str(),
        repair,
        ?repair_scope,
        "Starting Solana pipeline"
    );

    let runtime = SolanaChainRuntime::build(config, chain, shared_db_pool, handler_filter).await?;

    let channel_capacity = config
        .raw_data_collection
        .channel_capacity
        .unwrap_or(defaults::raw_data::CHANNEL_CAPACITY);
    let range_size = config
        .raw_data_collection
        .parquet_block_range
        .unwrap_or(1000) as u64;

    // --- Create channels ---

    // Decoder input channels (raw data → decoders)
    let (event_decoder_tx, event_decoder_rx) = if runtime.features.has_events {
        let (tx, rx) = mpsc::channel::<DecoderMessage>(channel_capacity);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    let (instr_decoder_tx, instr_decoder_rx) = if runtime.features.has_instructions {
        let (tx, rx) = mpsc::channel::<DecoderMessage>(channel_capacity);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    // Historical Solana runs now transform from decoded parquet after decode completes.
    let transform_events_tx: Option<mpsc::Sender<DecodedEventsMessage>> = None;
    let transform_complete_tx: Option<
        mpsc::Sender<crate::transformations::engine::RangeCompleteMessage>,
    > = None;

    // --- Spawn tasks ---

    let mut tasks: JoinSet<anyhow::Result<()>> = JoinSet::new();

    // Build function filter from repair scope before moving repair_scope
    let function_filter = if repair {
        repair_scope
            .as_ref()
            .and_then(|s| s.functions.as_ref())
            .map(|f| Arc::new(f.clone()))
    } else {
        None
    };

    // Backfill task
    {
        let chain_name = runtime.chain.name.clone();
        let rpc_client = runtime.rpc_client.clone();
        let programs = runtime.chain.solana_programs.clone();
        let historical_provider = runtime.chain.historical_provider;
        let configured_programs = runtime.configured_programs.clone();
        let event_tx = event_decoder_tx.clone();
        let instr_tx = instr_decoder_tx.clone();
        let backfill_repair_scope = if repair { repair_scope.clone() } else { None };

        tasks.spawn(async move {
            let result = signature_driven_backfill(
                &chain_name,
                &rpc_client,
                historical_provider,
                &programs,
                range_size,
                &configured_programs,
                event_tx.as_ref(),
                instr_tx.as_ref(),
                backfill_repair_scope.as_ref(),
            )
            .await;

            // Drop our senders to signal downstream that backfill is done.
            // The decoder tasks will see channel close after all senders are dropped.
            drop(event_tx);
            drop(instr_tx);

            result.map_err(|e| anyhow::anyhow!("Solana backfill failed: {}", e))
        });
    }

    // Event decoder task — decoder receives fully merged data for each range
    // (no source scope needed). Only function filtering makes output partial.
    if let Some(event_rx) = event_decoder_rx {
        let event_decoder = Arc::new(SolanaEventDecoder::new(runtime.decoders.clone()));
        let program_names = runtime.program_names.clone();
        let tx = transform_events_tx.clone();
        let complete = transform_complete_tx.clone();
        let chain_name = runtime.chain.name.clone();
        let ff = function_filter.clone();

        tasks.spawn(async move {
            decode_solana_events(
                event_decoder,
                program_names,
                event_rx,
                tx,
                complete,
                chain_name,
                ff,
                None,
                None,
                false,
            )
            .await;
            Ok(())
        });
    }

    // Drop the original event_decoder_tx so only the backfill task's clone remains
    drop(event_decoder_tx);

    // Instruction decoder task
    if let Some(instr_rx) = instr_decoder_rx {
        let instr_decoder = Arc::new(SolanaInstructionDecoder::new(runtime.decoders.clone()));
        let program_names = runtime.program_names.clone();
        let tx = transform_events_tx.clone();
        let complete = transform_complete_tx.clone();
        let chain_name = runtime.chain.name.clone();
        let ff = function_filter.clone();

        tasks.spawn(async move {
            decode_solana_instructions(
                instr_decoder,
                program_names,
                instr_rx,
                tx,
                complete,
                chain_name,
                ff,
                None,
                None,
            )
            .await;
            Ok(())
        });
    }

    // Drop remaining transform senders so channels close when decoder tasks finish
    drop(transform_events_tx);
    drop(transform_complete_tx);

    // Drop the original instr_decoder_tx so only the backfill task's clone remains
    drop(instr_decoder_tx);

    // --- Wait for all tasks ---

    while let Some(result) = tasks.join_next().await {
        result.context("Solana pipeline task panicked")??;
    }

    tracing::info!(
        chain = chain.name.as_str(),
        "Solana historical pipeline complete"
    );

    if runtime.transformations_enabled {
        crate::solana::historical_accounts::rebuild_cpmm_historical_account_states(
            chain,
            if repair { repair_scope.as_ref() } else { None },
        )
        .context("failed to rebuild historical CPMM account states")?;

        let tc = config
            .transformations
            .as_ref()
            .expect("transformations_enabled requires transformations config");
        let mode = if tc.mode.batch_for_catchup {
            ExecutionMode::Batch {
                batch_size: tc.mode.catchup_batch_size,
            }
        } else {
            ExecutionMode::Streaming
        };

        let engine = TransformationEngine::new(
            runtime.registry.clone(),
            runtime.db_pool.clone().unwrap(),
            None,
            TransformationEngineConfig {
                chain_name: chain.name.clone(),
                chain_id: chain.chain_id as u64,
                chain_type: chain.chain_type,
                mode,
                contracts: Contracts::new(),
                factory_collections: FactoryCollections::new(),
                handler_concurrency: tc.handler_concurrency,
                expect_log_completion: false,
                expect_eth_call_completion: false,
                expect_account_state_completion: false,
                expect_instruction_completion: false,
                from_block: chain.from_block,
                to_block: chain.to_block,
            },
            None,
        )
        .await
        .context("failed to create transformation engine")?;

        engine
            .initialize()
            .await
            .context("failed to initialize transformation engine")?;
        engine
            .run_catchup()
            .await
            .map_err(|e| anyhow::anyhow!("transformation catchup error: {}", e))?;
    }

    // --- Live mode ---

    if !catch_up_only {
        let live_mode = config.raw_data_collection.live_mode.unwrap_or(false);
        if live_mode {
            run_solana_live_mode(config, &runtime).await?;
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Live mode pipeline
// ---------------------------------------------------------------------------

fn collect_incomplete_live_slots(scan: &SolanaCatchupScanResult) -> BTreeSet<u64> {
    let mut slots = BTreeSet::new();
    slots.extend(
        scan.slots_needing_collection_resume
            .iter()
            .map(|request| request.slot),
    );
    slots.extend(scan.slots_needing_event_decode.iter().copied());
    slots.extend(scan.slots_needing_instruction_decode.iter().copied());
    slots.extend(scan.slots_needing_account_reads.iter().copied());
    slots.extend(scan.slots_needing_transform.iter().map(|(slot, _)| *slot));
    slots
}

fn compute_live_expected_start_slot(
    tip_after_cleanup: Option<u64>,
    deleted_slots: &BTreeSet<u64>,
) -> Option<u64> {
    tip_after_cleanup.or_else(|| {
        deleted_slots
            .first()
            .copied()
            .and_then(|slot| slot.checked_sub(1))
    })
}

/// Cadence for a per-slot account read group.
#[derive(Debug, Clone)]
pub enum AccountReadCadence {
    EverySlot,
    EveryNSlots(u64),
    Duration { seconds: u64 },
}

/// Build the set of `(program_name, account_type)` pairs whose configured
/// [`Frequency`] means they should be read on per-slot triggers
/// (`EveryBlock`, `EveryNBlocks`, `Duration`).
fn build_per_slot_account_types(programs: &SolanaPrograms) -> HashMap<String, HashSet<String>> {
    use crate::types::config::eth_call::Frequency;

    programs
        .iter()
        .filter_map(|(name, config)| {
            let accounts = config.accounts.as_ref()?;
            let types: HashSet<String> = accounts
                .iter()
                .filter(|a| {
                    matches!(
                        a.frequency,
                        Frequency::EveryBlock | Frequency::EveryNBlocks(_) | Frequency::Duration(_)
                    )
                })
                .map(|a| a.account_type.clone())
                .collect();
            if types.is_empty() {
                None
            } else {
                Some((name.clone(), types))
            }
        })
        .collect()
}

/// Build cadence info for per-slot account types.
/// Returns a map from `(program_name, account_type)` → `AccountReadCadence`.
fn build_cadence_map(programs: &SolanaPrograms) -> HashMap<(String, String), AccountReadCadence> {
    use crate::types::config::eth_call::Frequency;

    let mut map = HashMap::new();
    for (name, config) in programs {
        if let Some(ref accounts) = config.accounts {
            for a in accounts {
                let cadence = match &a.frequency {
                    Frequency::EveryBlock => AccountReadCadence::EverySlot,
                    Frequency::EveryNBlocks(n) => AccountReadCadence::EveryNSlots(*n),
                    Frequency::Duration(s) => AccountReadCadence::Duration { seconds: *s },
                    _ => continue, // Once / OnEvents not in per-slot path
                };
                map.insert((name.clone(), a.account_type.clone()), cadence);
            }
        }
    }
    map
}

/// Build the set of Once-frequency account types (program → account_types).
fn build_once_account_types(programs: &SolanaPrograms) -> HashMap<String, HashSet<String>> {
    use crate::types::config::eth_call::Frequency;

    programs
        .iter()
        .filter_map(|(name, config)| {
            let accounts = config.accounts.as_ref()?;
            let types: HashSet<String> = accounts
                .iter()
                .filter(|a| matches!(a.frequency, Frequency::Once))
                .map(|a| a.account_type.clone())
                .collect();
            if types.is_empty() {
                None
            } else {
                Some((name.clone(), types))
            }
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct OnEventAccountReadTarget {
    owner_program: String,
    account_type: String,
}

type OnEventsLookup = HashMap<(String, String), Vec<OnEventAccountReadTarget>>;

/// Build the OnEvents lookup: `(trigger_source, decoded_event_name)` → list of
/// account owner/account type pairs whose known addresses should be read when
/// that event fires.
fn build_on_events_lookup(programs: &SolanaPrograms) -> OnEventsLookup {
    use crate::types::config::eth_call::Frequency;

    let mut map: OnEventsLookup = HashMap::new();
    for (owner_program, config) in programs {
        if let Some(ref accounts) = config.accounts {
            for a in accounts {
                if let Frequency::OnEvents(ref triggers) = a.frequency {
                    for trigger in triggers.iter() {
                        let key = (trigger.source.clone(), extract_event_name(&trigger.event));
                        map.entry(key).or_default().push(OnEventAccountReadTarget {
                            owner_program: owner_program.clone(),
                            account_type: a.account_type.clone(),
                        });
                    }
                }
            }
        }
    }
    map
}

fn collect_on_event_account_read_triggers(
    slot: u64,
    block_time: Option<i64>,
    trigger_source_name: &str,
    events: &[DiscoveryEventData],
    on_events_lookup: &OnEventsLookup,
    discovery_manager: &DiscoveryManager,
    already_enqueued_targets: &mut HashSet<OnEventAccountReadTarget>,
) -> Vec<AccountReadTrigger> {
    let mut triggers = Vec::new();

    for event in events {
        let key = (trigger_source_name.to_string(), event.event_name.clone());
        let Some(targets) = on_events_lookup.get(&key) else {
            continue;
        };

        for target in targets {
            let known_addresses =
                discovery_manager.known_addresses(&target.owner_program, &target.account_type);
            if known_addresses.is_empty() {
                continue;
            }
            if !already_enqueued_targets.insert(target.clone()) {
                continue;
            }

            triggers.push(AccountReadTrigger {
                slot,
                block_time,
                source_name: target.owner_program.clone(),
                addresses: known_addresses
                    .into_iter()
                    .map(|address| address.to_bytes())
                    .collect(),
                await_completion_barrier: true,
                mark_complete_only: false,
            });
        }
    }

    triggers
}

/// In live mode, defer slot-level account completion through the discovery loop
/// whenever decoded events can enqueue account reads after the collector's
/// per-slot trigger. This covers both newly discovered addresses and OnEvents
/// reads.
fn should_defer_live_account_completion(features: &SolanaChainFeatures) -> bool {
    features.has_account_reads
        && features.has_events
        && (features.has_discovery || features.has_on_events_account_reads)
}

fn decoded_events_block_time(
    events: &[crate::transformations::context::DecodedEvent],
) -> Option<i64> {
    events
        .first()
        .and_then(|event| i64::try_from(event.block_timestamp).ok())
}

fn build_live_decoder_destinations(
    has_discovery: bool,
    transform_events_tx: Option<mpsc::Sender<DecodedEventsMessage>>,
    discovery_events_tx: Option<mpsc::Sender<DecodedEventsMessage>>,
) -> (
    Option<mpsc::Sender<DecodedEventsMessage>>,
    Option<mpsc::Sender<DecodedEventsMessage>>,
) {
    let event_decoder_dest = if has_discovery {
        discovery_events_tx.clone()
    } else {
        transform_events_tx.clone()
    };
    let instruction_decoder_dest = if has_discovery {
        discovery_events_tx
    } else {
        transform_events_tx
    };

    (event_decoder_dest, instruction_decoder_dest)
}

fn build_startup_once_account_read_trigger(
    slot: u64,
    block_time: Option<i64>,
    source_name: String,
    addresses: Vec<[u8; 32]>,
) -> AccountReadTrigger {
    AccountReadTrigger {
        slot,
        block_time,
        source_name,
        addresses,
        await_completion_barrier: false,
        mark_complete_only: false,
    }
}

async fn resolve_startup_once_trigger_metadata(
    rpc_client: &SolanaRpcClient,
    live_storage: &SolanaLiveStorage,
) -> (u64, Option<i64>) {
    match rpc_client.get_slot().await {
        Ok(slot) => {
            let block_time = match rpc_client.get_block(slot).await {
                Ok(Some(block)) => block.block_time,
                Ok(None) => None,
                Err(error) => {
                    tracing::warn!(
                        slot,
                        error = %error,
                        "Failed to fetch block metadata for startup Once account reads",
                    );
                    None
                }
            };
            (slot, block_time)
        }
        Err(error) => {
            tracing::warn!(
                error = %error,
                "Failed to fetch current slot for startup Once account reads",
            );
            if let Ok(Some(slot)) = live_storage.max_slot_number() {
                let block_time = live_storage.read_slot(slot).ok().and_then(|s| s.block_time);
                (slot, block_time)
            } else {
                (0, None)
            }
        }
    }
}

/// Run the Solana live mode pipeline.
///
/// 1. Set up live storage and reorg detector
/// 2. Subscribe to slot notifications via WebSocket
/// 3. Spawn live collector, decoder tasks, compaction service
/// 4. Wait for all tasks (runs until shutdown)
async fn run_solana_live_mode(
    config: &IndexerConfig,
    runtime: &SolanaChainRuntime,
) -> anyhow::Result<()> {
    let chain = &runtime.chain;

    // Resolve WebSocket URL
    let ws_url = match &chain.ws_url_env_var {
        Some(env_var) => std::env::var(env_var).with_context(|| {
            format!(
                "env var {} not set for Solana WebSocket (required for live mode)",
                env_var
            )
        })?,
        None => {
            anyhow::bail!(
                "ws_url_env_var not configured for chain '{}' — required for live mode",
                chain.name
            );
        }
    };

    // Reorg depth based on commitment level
    let reorg_depth = match chain.commitment {
        SolanaCommitment::Processed => 150,
        SolanaCommitment::Confirmed => 32,
        SolanaCommitment::Finalized => 0,
    };

    let range_size = config
        .raw_data_collection
        .parquet_block_range
        .unwrap_or(1000) as u64;

    let live_config = LiveModeConfig {
        reorg_depth,
        compaction_interval_secs: 60,
        range_size,
        transform_retry_grace_period_secs: 120,
    };

    let channel_capacity = config
        .raw_data_collection
        .channel_capacity
        .unwrap_or(defaults::raw_data::CHANNEL_CAPACITY);

    // Build pipeline expectations from features
    let expectations = SolanaLivePipelineExpectations {
        expect_event_decode: runtime.features.has_events,
        expect_instruction_decode: runtime.features.has_instructions,
        expect_account_reads: runtime.features.has_account_reads,
        expect_transformations: runtime.transformations_enabled,
    };

    // Set up live storage
    let live_storage = SolanaLiveStorage::new(&chain.name);
    live_storage
        .ensure_dirs()
        .context("failed to create live storage directories")?;

    // Set up reorg detector
    let mut reorg_detector = SolanaReorgDetector::new(reorg_depth);

    // Seed from existing storage
    let recent_slots = live_storage.get_recent_slots_for_reorg(reorg_depth)?;
    reorg_detector.seed(
        recent_slots
            .iter()
            .map(|s| (s.slot, s.parent_slot, s.blockhash)),
    );

    // Run catchup scan for incomplete slots from previous session.
    // Delete incomplete slots so the collector re-fetches them when it
    // sees the gap between the cleaned-up tip and the first WebSocket slot.
    let mut deleted_slots = BTreeSet::new();
    {
        let registered_handlers: HashSet<String> =
            if let Some(ref tracker) = runtime.progress_tracker {
                tracker.lock().await.handler_keys().clone()
            } else {
                HashSet::new()
            };

        let catchup = SolanaLiveCatchupService::new(&chain.name, registered_handlers, expectations);

        let reconstructed = catchup.reconstruct_missing_status_files()?;
        if reconstructed > 0 {
            tracing::info!(
                chain = chain.name.as_str(),
                reconstructed,
                "Reconstructed missing status files"
            );
        }

        let scan = catchup.scan_incomplete_slots()?;
        if !scan.is_empty() {
            // Delete every incomplete slot so the collector re-fetches it.
            // This moves the effective live tip back to the last complete slot,
            // allowing first-slot gap detection to replay the cleaned ranges.
            for slot in collect_incomplete_live_slots(&scan) {
                match live_storage.delete_all(slot) {
                    Ok(()) => {
                        deleted_slots.insert(slot);
                    }
                    Err(e) => {
                        tracing::warn!(
                            slot,
                            error = %e,
                            "Failed to delete incomplete slot during catchup cleanup"
                        );
                    }
                }
            }

            tracing::info!(
                chain = chain.name.as_str(),
                collection_resume = scan.slots_needing_collection_resume.len(),
                event_decode = scan.slots_needing_event_decode.len(),
                instruction_decode = scan.slots_needing_instruction_decode.len(),
                account_reads = scan.slots_needing_account_reads.len(),
                transform = scan.slots_needing_transform.len(),
                deleted = deleted_slots.len(),
                "Catchup: deleted incomplete slots for re-collection"
            );
        }
    }

    // Reload handler progress from persisted status files so that compaction
    // can recognize slots that completed transformation before the last crash.
    if let Some(ref tracker) = runtime.progress_tracker {
        let mut guard = tracker.lock().await;
        for slot in live_storage.list_statuses().unwrap_or_default() {
            if let Ok(status) = live_storage.read_status(slot) {
                guard.restore_completed(slot, status.completed_handlers);
            }
        }
        tracing::info!(
            chain = chain.name.as_str(),
            "Reloaded handler progress from persisted status files",
        );
    }

    let expected_start_slot =
        compute_live_expected_start_slot(live_storage.max_slot_number()?, &deleted_slots);

    // Detect internal gaps left after catchup deletions (e.g. slot 100 deleted
    // while slot 200 survives). The collector backfills these before the WS loop.
    let startup_gaps = live_storage.find_gaps()?;

    // Load known-account state whenever account reads are configured so
    // persisted addresses remain available even without discovery rules.
    let discovery_manager = if runtime.features.has_discovery || runtime.features.has_account_reads
    {
        let mut manager = DiscoveryManager::load_persisted(&chain.name, &chain.solana_programs);
        tracing::info!(
            chain = chain.name.as_str(),
            "Loaded discovery manager with persisted state"
        );

        // Bootstrap from getProgramAccounts if this is the first run
        if let Err(e) = manager
            .bootstrap(&runtime.rpc_client, &chain.solana_programs)
            .await
        {
            tracing::warn!(
                chain = chain.name.as_str(),
                error = %e,
                "Discovery bootstrap failed (will rely on event-driven discovery)"
            );
        }

        Some(manager)
    } else {
        None
    };

    // Build frequency-derived lookups from config.
    let per_slot_account_types = build_per_slot_account_types(&chain.solana_programs);
    let once_account_types = build_once_account_types(&chain.solana_programs);
    let on_events_lookup = build_on_events_lookup(&chain.solana_programs);
    let cadence_map = build_cadence_map(&chain.solana_programs);

    // Shared registry for known account addresses whose frequency requires
    // per-slot reads. Preserves (program, account_type) grouping for cadence.
    let known_accounts_registry: Option<SharedKnownAccounts> =
        discovery_manager.as_ref().map(|m| {
            Arc::new(std::sync::RwLock::new(
                m.snapshot_filtered(&per_slot_account_types),
            ))
        });

    // Once-frequency addresses that need an initial read at startup.
    let once_startup_addresses: Vec<(String, String, Vec<[u8; 32]>)> = discovery_manager
        .as_ref()
        .map(|m| {
            let snapshot = m.snapshot_filtered(&once_account_types);
            snapshot
                .into_iter()
                .flat_map(|(prog, types)| {
                    types
                        .into_iter()
                        .map(move |(acct_type, addrs)| (prog.clone(), acct_type, addrs))
                })
                .collect()
        })
        .unwrap_or_default();
    let once_startup_trigger_metadata = if once_startup_addresses.is_empty() {
        None
    } else {
        Some(resolve_startup_once_trigger_metadata(&runtime.rpc_client, &live_storage).await)
    };

    tracing::info!(
        chain = chain.name.as_str(),
        reorg_depth,
        commitment = ?chain.commitment,
        seeded_slots = reorg_detector.tracked_count(),
        last_slot = ?expected_start_slot,
        "Starting Solana live mode"
    );

    // --- Create channels ---

    let (ws_tx, ws_rx) = mpsc::unbounded_channel();
    // Receiver is dropped immediately so live_tx.send() returns Err
    // instead of blocking after the buffer fills. The collector already
    // handles send errors gracefully. A real consumer will be wired in
    // when Solana live handlers are added.
    let (live_tx, _) = mpsc::channel(channel_capacity);

    // Decoder channels (collector → decoders)
    let (event_decoder_tx, event_decoder_rx) = if runtime.features.has_events {
        let (tx, rx) = mpsc::channel::<DecoderMessage>(channel_capacity);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    let (instr_decoder_tx, instr_decoder_rx) = if runtime.features.has_instructions {
        let (tx, rx) = mpsc::channel::<DecoderMessage>(channel_capacity);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    // Transformation channels (decoders → engine)
    let (transform_events_tx, transform_events_rx) = if runtime.transformations_enabled {
        let (tx, rx) = mpsc::channel(channel_capacity);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    let (transform_complete_tx, transform_complete_rx) = if runtime.transformations_enabled {
        let (tx, rx) = mpsc::channel(channel_capacity);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    // Reorg channel (collector → engine)
    let (transform_reorg_tx, transform_reorg_rx) = if runtime.transformations_enabled {
        let (tx, rx) = mpsc::channel::<ReorgMessage>(channel_capacity);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    // Retry channel (compaction → engine)
    let (transform_retry_tx, transform_retry_rx) = if runtime.transformations_enabled {
        let (tx, rx) = mpsc::channel(channel_capacity);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    // Discovery channels — the bridge task (spawned below) taps decoded events
    // and forwards DiscoveryMessages to this channel.
    let needs_event_driven_account_reads =
        runtime.features.has_discovery || runtime.features.has_on_events_account_reads;

    let (discovery_tx, discovery_rx) = if needs_event_driven_account_reads {
        let (tx, rx) = mpsc::channel(channel_capacity);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    // Account reader channels
    let (account_states_tx, account_states_rx) = if runtime.features.has_account_reads {
        let (tx, rx) = mpsc::channel(channel_capacity);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };
    let (account_trigger_tx, account_trigger_rx) = if runtime.features.has_account_reads {
        let (tx, rx) = mpsc::channel::<AccountReadTrigger>(channel_capacity);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    // --- Spawn tasks ---

    let mut tasks: JoinSet<anyhow::Result<()>> = JoinSet::new();

    // WebSocket subscription
    let ws_client = Arc::new(SolanaWsClient::with_default_config(
        &ws_url,
        runtime.rpc_client.clone(),
    ));
    {
        let handle = ws_client.clone().subscribe(ws_tx);
        tasks.spawn(async move {
            handle
                .await
                .map_err(|e| anyhow::anyhow!("WebSocket task panicked: {}", e))?
                .map_err(|e| anyhow::anyhow!("WebSocket subscription error: {}", e))
        });
    }

    // Live collector
    {
        let collector = SolanaLiveCollector::new(
            chain.name.clone(),
            runtime.rpc_client.clone(),
            live_storage.clone(),
            reorg_detector,
            live_config.clone(),
            runtime.configured_programs.clone(),
            runtime.progress_tracker.clone(),
            expected_start_slot,
            expectations,
        )
        .with_startup_backfill_gaps(startup_gaps);

        let lt = live_tx.clone();
        let edt = event_decoder_tx.clone();
        let idt = instr_decoder_tx.clone();
        let trt = transform_reorg_tx.clone();
        let att = account_trigger_tx.clone();

        tasks.spawn(async move {
            collector
                .run(ws_rx, lt, edt, idt, trt, att)
                .await
                .map_err(|e| anyhow::anyhow!("Solana live collector error: {}", e))
        });
        // Drop the original sender so the engine's reorg input closes when
        // the collector (holding the clone) exits.
        drop(transform_reorg_tx);
    }

    // When discovery is active, decoders write to an intermediate channel.
    // A bridge task forwards to both the transform engine and the discovery
    // manager. When discovery is inactive, decoders write to transform_events_tx
    // directly.
    let (decoder_events_tx, decoder_events_rx) = if needs_event_driven_account_reads {
        let (tx, rx) = mpsc::channel(channel_capacity);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    // Discovery is event-only. When enabled, decoded events flow through the
    // bridge so discovery/account reads can inspect them before the engine.
    // Decoded instructions always go directly to the engine.
    let (event_decoder_dest_tx, instruction_decoder_dest_tx) = build_live_decoder_destinations(
        needs_event_driven_account_reads,
        transform_events_tx.clone(),
        decoder_events_tx.clone(),
    );

    // Event decoder task (live mode)
    let defer_live_account_completion = should_defer_live_account_completion(&runtime.features);
    if let Some(event_rx) = event_decoder_rx {
        let event_decoder = Arc::new(SolanaEventDecoder::new(runtime.decoders.clone()));
        let program_names = runtime.program_names.clone();
        let tx = event_decoder_dest_tx.clone();
        let complete = transform_complete_tx.clone();
        let chain_name = runtime.chain.name.clone();
        let storage_for_decoder = live_storage.clone();

        tasks.spawn(async move {
            decode_solana_events(
                event_decoder,
                program_names,
                event_rx,
                tx,
                complete,
                chain_name,
                None,
                None,
                Some(storage_for_decoder),
                defer_live_account_completion,
            )
            .await;
            Ok(())
        });
    }
    drop(event_decoder_tx);

    // Instruction decoder task (live mode)
    if let Some(instr_rx) = instr_decoder_rx {
        let instr_decoder = Arc::new(SolanaInstructionDecoder::new(runtime.decoders.clone()));
        let program_names = runtime.program_names.clone();
        let tx = instruction_decoder_dest_tx.clone();
        let complete = transform_complete_tx.clone();
        let chain_name = runtime.chain.name.clone();
        let storage_for_decoder = live_storage.clone();

        tasks.spawn(async move {
            decode_solana_instructions(
                instr_decoder,
                program_names,
                instr_rx,
                tx,
                complete,
                chain_name,
                None,
                None,
                Some(storage_for_decoder),
            )
            .await;
            Ok(())
        });
    }
    drop(instr_decoder_tx);
    drop(event_decoder_dest_tx);
    drop(instruction_decoder_dest_tx);
    drop(decoder_events_tx);
    // Save a clone for the account reader before dropping the original
    let account_reader_complete_tx = transform_complete_tx.clone();
    drop(transform_complete_tx);

    // Discovery bridge task: reads decoded events from the intermediate channel,
    // forwards to both the transform engine and the discovery manager.
    if let (Some(mut bridge_rx), Some(disc_tx)) = (decoder_events_rx, discovery_tx) {
        let engine_tx = transform_events_tx.clone();

        tasks.spawn(async move {
            use crate::solana::discovery::{
                DiscoveryEventData, DiscoveryFieldValue, DiscoveryMessage,
            };
            use crate::transformations::engine::DecodedEventsMessage;

            while let Some(msg) = bridge_rx.recv().await {
                // Slot-done sentinel: empty source_name + empty events.
                // Sent by the decoder after all decoded groups for a slot.
                // Forward as RangeComplete to the discovery loop (same
                // channel as events → guarantees ordering).
                if msg.source_name.is_empty() && msg.events.is_empty() {
                    let _ = disc_tx
                        .send(DiscoveryMessage::RangeComplete {
                            range_start: msg.range_start,
                            range_end: msg.range_end,
                        })
                        .await;
                    continue;
                }

                // Forward to engine
                if let Some(ref tx) = engine_tx {
                    let engine_msg = DecodedEventsMessage {
                        range_start: msg.range_start,
                        range_end: msg.range_end,
                        source_name: msg.source_name.clone(),
                        event_name: msg.event_name.clone(),
                        events: msg.events.clone(),
                    };
                    if tx.send(engine_msg).await.is_err() {
                        tracing::debug!("Transform events channel closed in bridge");
                    }
                }

                // Convert to DiscoveryMessage and forward to discovery manager
                let discovery_events: Vec<DiscoveryEventData> = msg
                    .events
                    .iter()
                    .map(|e| {
                        let fields = e
                            .params
                            .iter()
                            .map(|(k, v)| {
                                let field_val = match v.as_pubkey() {
                                    Some(bytes) => DiscoveryFieldValue::Pubkey(bytes),
                                    None => DiscoveryFieldValue::Other,
                                };
                                (k.to_string(), field_val)
                            })
                            .collect();
                        DiscoveryEventData {
                            event_name: e.event_name.clone(),
                            fields,
                        }
                    })
                    .collect();
                let block_time = decoded_events_block_time(&msg.events);

                if !discovery_events.is_empty() {
                    let _ = disc_tx
                        .send(DiscoveryMessage::DecodedEvents {
                            range_start: msg.range_start,
                            range_end: msg.range_end,
                            block_time,
                            source_name: msg.source_name,
                            events: discovery_events,
                        })
                        .await;
                }
            }
            Ok(())
        });
    }
    drop(transform_events_tx);

    // Discovery manager loop — consumes DiscoveryMessages, drives address
    // discovery, and triggers discovery-managed account reads.
    if let (Some(mut manager_instance), Some(mut rx)) = (discovery_manager, discovery_rx) {
        let chain_name = chain.name.clone();
        let account_tx = account_trigger_tx.clone();
        let registry_for_discovery = known_accounts_registry.clone();
        let per_slot_types_for_discovery = per_slot_account_types.clone();
        let on_events_for_discovery = on_events_lookup.clone();
        let defer_live_account_completion = defer_live_account_completion;
        let mut seen_on_event_targets_by_slot: HashMap<u64, HashSet<OnEventAccountReadTarget>> =
            HashMap::new();

        tasks.spawn(async move {
            use crate::solana::discovery::DiscoveryMessage;

            while let Some(msg) = rx.recv().await {
                match msg {
                    DiscoveryMessage::DecodedEvents {
                        range_start,
                        block_time,
                        source_name,
                        events,
                        ..
                    } => {
                        // 1. Process for address discovery
                        let new_addrs = manager_instance.process_events(&source_name, &events);
                        if !new_addrs.is_empty() {
                            if let Err(e) = manager_instance.persist(&chain_name) {
                                tracing::warn!(
                                    chain = chain_name.as_str(),
                                    error = %e,
                                    "Failed to persist discovery state after new addresses",
                                );
                            }
                            // Update the shared per-slot registry
                            if let Some(ref registry) = registry_for_discovery {
                                *registry.write().unwrap() = manager_instance
                                    .snapshot_filtered(&per_slot_types_for_discovery);
                            }
                        }
                        // Send triggers for newly discovered addresses (Once reads)
                        for addrs in new_addrs {
                            if let Some(ref tx) = account_tx {
                                let addresses: Vec<[u8; 32]> =
                                    addrs.addresses.iter().map(|p| p.to_bytes()).collect();
                                if !addresses.is_empty() {
                                    let _ = tx
                                        .send(AccountReadTrigger {
                                            slot: range_start,
                                            block_time,
                                            source_name: addrs.program_name.clone(),
                                            addresses,
                                            await_completion_barrier: true,
                                            mark_complete_only: false,
                                        })
                                        .await;
                                }
                            }
                        }

                        // 2. Check OnEvents rules: if any decoded event matches
                        //    an OnEvents trigger, read all known addresses for the
                        //    matching account types.
                        if !on_events_for_discovery.is_empty() {
                            if let Some(ref tx) = account_tx {
                                let seen_targets =
                                    seen_on_event_targets_by_slot.entry(range_start).or_default();
                                for trigger in collect_on_event_account_read_triggers(
                                    range_start,
                                    block_time,
                                    &source_name,
                                    &events,
                                    &on_events_for_discovery,
                                    &manager_instance,
                                    seen_targets,
                                ) {
                                    let _ = tx.send(trigger).await;
                                }
                            }
                        }
                    }
                    DiscoveryMessage::RangeComplete {
                        range_start, ..
                    } => {
                        seen_on_event_targets_by_slot.remove(&range_start);
                        // All events for this slot have been decoded and
                        // forwarded through this loop. Any discovery-triggered
                        // reads for this slot have already been enqueued on
                        // account_tx above.
                        // Now signal the reader that account reads are done.
                        if defer_live_account_completion {
                            if let Some(ref tx) = account_tx {
                                let _ = tx
                                    .send(AccountReadTrigger {
                                        slot: range_start,
                                        block_time: None,
                                        source_name: String::new(),
                                        addresses: Vec::new(),
                                        await_completion_barrier: false,
                                        mark_complete_only: true,
                                    })
                                    .await;
                            }
                        }
                    }
                    DiscoveryMessage::AllComplete => break,
                }
            }

            if let Err(e) = manager_instance.persist(&chain_name) {
                tracing::warn!(chain = chain_name.as_str(), error = %e, "Failed to persist discovery state");
            }
            Ok(())
        });
    }
    // Live account reader — runs whenever account reads are configured,
    // regardless of whether transformations are enabled, so that slots
    // can reach the accounts_read/accounts_decoded completion flags.
    // Spawned BEFORE the Once startup sends so the consumer is draining.
    if let Some(trigger_rx) = account_trigger_rx {
        let account_decoder = Arc::new(SolanaAccountDecoder::new(runtime.decoders.clone()));
        let reader = SolanaLiveAccountReader::new(
            runtime.rpc_client.clone(),
            account_decoder,
            live_storage.clone(),
            runtime.program_names.clone(),
            known_accounts_registry.clone(),
            cadence_map.clone(),
            defer_live_account_completion,
        );

        // Discovery feeds through trigger_tx, so the reader only needs trigger_rx.
        let (_disc_tx, disc_rx) = mpsc::channel(1);

        // Route decoded states and completion signals into the transform engine
        // channels when available, otherwise create stubs so the reader can run.
        // When the engine is not running (!transformations_enabled), use a
        // dropped-receiver stub so sends fail immediately instead of blocking
        // after the buffer fills.
        let states_tx = if runtime.transformations_enabled {
            account_states_tx
                .clone()
                .unwrap_or_else(|| mpsc::channel(1).0)
        } else {
            mpsc::channel(1).0
        };
        let complete_tx = if runtime.transformations_enabled {
            account_reader_complete_tx.unwrap_or_else(|| mpsc::channel(1).0)
        } else {
            mpsc::channel(1).0
        };

        tasks.spawn(async move {
            reader
                .run(disc_rx, trigger_rx, states_tx, complete_tx)
                .await
                .map_err(|e| anyhow::anyhow!("Account reader error: {}", e))
        });
    }

    // Send initial read triggers for Once-frequency addresses that were
    // loaded from persisted discovery state or found during bootstrap.
    // Sent after the reader is spawned so the consumer is draining the channel.
    if !once_startup_addresses.is_empty() {
        if let Some(ref tx) = account_trigger_tx {
            let (startup_slot, startup_block_time) =
                once_startup_trigger_metadata.unwrap_or((0, None));
            for (program_name, _acct_type, addresses) in &once_startup_addresses {
                if addresses.is_empty() {
                    continue;
                }
                tracing::info!(
                    chain = chain.name.as_str(),
                    source = %program_name,
                    count = addresses.len(),
                    "Sending initial Once read for startup-known accounts",
                );
                let _ = tx
                    .send(build_startup_once_account_read_trigger(
                        startup_slot,
                        startup_block_time,
                        program_name.clone(),
                        addresses.clone(),
                    ))
                    .await;
            }
        }
    }
    drop(account_trigger_tx);

    // Compaction service — always runs in live mode, even without transformation
    // handlers, since it's responsible for moving bincode → parquet and cleaning
    // up live storage.
    {
        // If no progress tracker exists (no handlers), create a stub tracker
        // so the compaction service can track slot completeness.
        let tracker = match &runtime.progress_tracker {
            Some(t) => t.clone(),
            None => {
                let mut t =
                    LiveProgressTracker::new(chain.chain_id as i64, None, chain.name.clone());
                t.set_status_storage(Arc::new(
                    crate::solana::live::storage::SolanaLiveStorage::new(&chain.name),
                ));
                Arc::new(Mutex::new(t))
            }
        };

        let mut compaction = SolanaCompactionService::new(
            chain.name.clone(),
            live_config.clone(),
            tracker,
            expectations,
        );
        // Keep Solana live retries disabled until RetryProcessor can replay
        // Solana live artifacts instead of the EVM-only retry inputs.
        tasks.spawn(async move {
            compaction.run().await;
            Ok(())
        });
    }

    // Transformation engine
    if runtime.transformations_enabled {
        let tc = config
            .transformations
            .as_ref()
            .expect("transformations_enabled requires transformations config");

        let engine_config = TransformationEngineConfig {
            chain_name: chain.name.clone(),
            chain_id: chain.chain_id as u64,
            chain_type: chain.chain_type,
            mode: ExecutionMode::Streaming,
            contracts: Contracts::new(),
            factory_collections: FactoryCollections::new(),
            handler_concurrency: tc.handler_concurrency,
            expect_log_completion: expectations.expect_event_decode,
            expect_eth_call_completion: false,
            expect_account_state_completion: expectations.expect_account_reads,
            expect_instruction_completion: expectations.expect_instruction_decode,
            from_block: chain.from_block,
            to_block: chain.to_block,
        };

        let engine = TransformationEngine::new(
            runtime.registry.clone(),
            runtime.db_pool.clone().unwrap(),
            None, // No EVM RPC client for Solana
            engine_config,
            runtime.progress_tracker.clone(),
        )
        .await
        .context("failed to create transformation engine")?;

        engine
            .initialize()
            .await
            .context("failed to initialize transformation engine")?;

        // Dummy calls channel (Solana has no eth_calls)
        let (dummy_calls_tx, dummy_calls_rx) = mpsc::channel(1);

        let events_rx = transform_events_rx.unwrap();
        let complete_rx = transform_complete_rx.unwrap();
        drop(dummy_calls_tx);

        tasks.spawn(async move {
            engine
                .run(
                    events_rx,
                    dummy_calls_rx,
                    account_states_rx,
                    complete_rx,
                    transform_reorg_rx,
                    transform_retry_rx,
                )
                .await
                .map_err(|e| anyhow::anyhow!("Transformation engine error: {}", e))
        });
        // No other task holds this sender (retry is disabled for Solana live);
        // drop it so the engine's retry input closes immediately.
        drop(transform_retry_tx);
    }

    // --- Run until shutdown ---

    tracing::info!(chain = chain.name.as_str(), "Solana live mode running");

    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(())) => {
                tracing::info!(chain = chain.name.as_str(), "Live mode task completed");
            }
            Ok(Err(e)) => {
                tracing::error!(chain = chain.name.as_str(), "Live mode task error: {}", e);
                return Err(e);
            }
            Err(e) => {
                tracing::error!(
                    chain = chain.name.as_str(),
                    "Live mode task panicked: {}",
                    e
                );
                return Err(anyhow::anyhow!("Live mode task panicked: {}", e));
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Live-only pipeline
// ---------------------------------------------------------------------------

/// Run Solana live mode without historical backfill.
///
/// Skips signature-driven backfill and goes straight to live slot processing.
pub async fn process_solana_chain_live_only(
    config: &IndexerConfig,
    chain: &ChainConfig,
    shared_db_pool: Option<Arc<DbPool>>,
    handler_filter: Option<&HashSet<String>>,
) -> anyhow::Result<()> {
    tracing::info!(
        chain = chain.name.as_str(),
        "Starting Solana live-only pipeline"
    );
    let runtime = SolanaChainRuntime::build(config, chain, shared_db_pool, handler_filter).await?;
    run_solana_live_mode(config, &runtime).await
}

/// Re-run transformation catchup for a Solana chain using existing decoded
/// parquet output only.
pub async fn transform_only_solana_chain(
    config: &IndexerConfig,
    chain: &ChainConfig,
    shared_db_pool: Option<Arc<DbPool>>,
    handler_filter: Option<&HashSet<String>>,
) -> anyhow::Result<()> {
    tracing::info!(
        chain = chain.name.as_str(),
        "Transformation-only mode for Solana chain"
    );

    let runtime = SolanaChainRuntime::build(config, chain, shared_db_pool, handler_filter).await?;

    if !runtime.transformations_enabled {
        tracing::info!(
            chain = chain.name.as_str(),
            "No transformation handlers registered for chain, nothing to do"
        );
        return Ok(());
    }

    let tc = config.transformations.as_ref().ok_or_else(|| {
        anyhow::anyhow!("transformations config required for --transformation-only mode")
    })?;

    let mode = if tc.mode.batch_for_catchup {
        ExecutionMode::Batch {
            batch_size: tc.mode.catchup_batch_size,
        }
    } else {
        ExecutionMode::Streaming
    };

    let registry = runtime.registry.clone();
    crate::solana::historical_accounts::rebuild_cpmm_historical_account_states(chain, None)
        .context("failed to rebuild historical CPMM account states")?;
    let engine = TransformationEngine::new(
        registry.clone(),
        runtime
            .db_pool
            .clone()
            .expect("transformations_enabled requires db"),
        None,
        TransformationEngineConfig {
            chain_name: chain.name.clone(),
            chain_id: chain.chain_id,
            chain_type: chain.chain_type,
            mode,
            contracts: Contracts::new(),
            factory_collections: FactoryCollections::new(),
            handler_concurrency: tc.handler_concurrency,
            expect_log_completion: false,
            expect_eth_call_completion: false,
            expect_account_state_completion: false,
            expect_instruction_completion: false,
            from_block: chain.from_block,
            to_block: chain.to_block,
        },
        None,
    )
    .await
    .context("failed to create transformation engine")?;

    engine
        .initialize()
        .await
        .context("failed to initialize transformation handlers")?;

    tracing::info!(
        chain = chain.name.as_str(),
        handlers = registry.handler_count(),
        "Running Solana transformation catchup"
    );

    engine
        .run_catchup()
        .await
        .map_err(|e| anyhow::anyhow!("transformation catchup error: {}", e))?;

    tracing::info!(
        chain = chain.name.as_str(),
        "Transformation-only processing complete for Solana chain"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Decode-only pipeline
// ---------------------------------------------------------------------------

/// Re-decode existing raw parquet files without collection or transformation.
///
/// Reads raw event and instruction parquet from disk, runs them through the
/// configured decoders, and writes decoded parquet output. Useful for
/// re-processing after IDL updates.
pub async fn decode_only_solana_chain(
    config: &IndexerConfig,
    chain: &ChainConfig,
    repair_scope: Option<RepairScope>,
) -> anyhow::Result<()> {
    tracing::info!(
        chain = chain.name.as_str(),
        ?repair_scope,
        "Decode-only mode for Solana chain"
    );

    let chain_arc = Arc::new(chain.clone());
    let decoders = build_decoders(&chain_arc)?;
    let program_names = Arc::new(build_program_names(&chain_arc)?);

    let channel_capacity = config
        .raw_data_collection
        .channel_capacity
        .unwrap_or(defaults::raw_data::CHANNEL_CAPACITY);

    // Build function filter from repair scope
    let function_filter = repair_scope
        .as_ref()
        .and_then(|s| s.functions.as_ref())
        .map(|f| Arc::new(f.clone()));

    // When source-scoped, restrict stale cleanup to the in-scope source
    // directories. Unknown directories (including old names from source
    // renames) are left untouched — we cannot distinguish an old name for
    // an in-scope program from one for an out-of-scope program without
    // reading parquet content. Source renames are cleaned up by running
    // unscoped --decode-only --repair.
    let only_sources: Option<Arc<HashSet<String>>> = repair_scope
        .as_ref()
        .and_then(|s| s.sources.as_ref())
        .map(|s| Arc::new(s.clone()));

    let mut tasks: JoinSet<anyhow::Result<()>> = JoinSet::new();

    // Event decoding
    if !decoders.is_empty() {
        let event_decoder = Arc::new(SolanaEventDecoder::new(decoders.clone()));
        let (tx, rx) = mpsc::channel::<DecoderMessage>(channel_capacity);
        let names = program_names.clone();
        let chain_name = chain.name.clone();
        let ff = function_filter.clone();
        let ss = only_sources.clone();

        tasks.spawn(async move {
            decode_solana_events(
                event_decoder,
                names,
                rx,
                None,
                None,
                chain_name,
                ff,
                ss,
                None,
                false,
            )
            .await;
            Ok(())
        });

        // Spawn reader task to feed raw event parquet files into the decoder channel
        let event_chain_name = chain.name.clone();
        let event_scope = repair_scope.clone();
        let event_program_names = program_names.clone();
        tasks.spawn(async move {
            feed_raw_events_to_decoder(
                &event_chain_name,
                tx,
                event_scope.as_ref(),
                event_program_names.as_ref(),
            )
            .await
        });
    }

    // Instruction decoding
    if !decoders.is_empty() {
        let instr_decoder = Arc::new(SolanaInstructionDecoder::new(decoders.clone()));
        let (tx, rx) = mpsc::channel::<DecoderMessage>(channel_capacity);
        let names = program_names.clone();
        let chain_name = chain.name.clone();
        let ff = function_filter.clone();
        let ss = only_sources.clone();

        tasks.spawn(async move {
            decode_solana_instructions(
                instr_decoder,
                names,
                rx,
                None,
                None,
                chain_name,
                ff,
                ss,
                None,
            )
            .await;
            Ok(())
        });

        // Spawn reader task to feed raw instruction parquet files into the decoder channel
        let instr_chain_name = chain.name.clone();
        let instr_scope = repair_scope.clone();
        let instr_program_names = program_names.clone();
        tasks.spawn(async move {
            feed_raw_instructions_to_decoder(
                &instr_chain_name,
                tx,
                instr_scope.as_ref(),
                instr_program_names.as_ref(),
            )
            .await
        });
    }

    while let Some(result) = tasks.join_next().await {
        result.context("Solana decode-only task panicked")??;
    }

    crate::solana::historical_accounts::rebuild_cpmm_historical_account_states(
        chain,
        repair_scope.as_ref(),
    )
    .context("failed to rebuild historical CPMM account states")?;

    tracing::info!(
        chain = chain.name.as_str(),
        "Solana decode-only processing complete"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Raw parquet file readers for decode-only mode
// ---------------------------------------------------------------------------

/// Read raw event parquet files from disk and send them as
/// [`DecoderMessage::SolanaEventsReady`] on the provided channel.
///
/// After all files are sent, sends [`DecoderMessage::AllComplete`] and drops
/// the sender.
async fn feed_raw_events_to_decoder(
    chain_name: &str,
    tx: mpsc::Sender<DecoderMessage>,
    repair_scope: Option<&RepairScope>,
    program_names: &HashMap<[u8; 32], String>,
) -> anyhow::Result<()> {
    let events_dir = raw_solana_events_dir(chain_name);

    let ranges = match scan_parquet_ranges(&events_dir) {
        Ok(r) => r,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            tracing::info!(
                chain = chain_name,
                "No raw events directory found, skipping event decode"
            );
            let _ = tx.send(DecoderMessage::AllComplete).await;
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };

    tracing::info!(
        chain = chain_name,
        files = ranges.len(),
        "Reading raw event parquet files for decode-only mode"
    );

    for (start, end_inclusive, path) in &ranges {
        let range_end = end_inclusive + 1;

        // Skip ranges outside the repair scope
        if let Some(scope) = repair_scope {
            if !scope.matches_range(*start, range_end) {
                continue;
            }
        }

        let events = tokio::task::spawn_blocking({
            let path = path.clone();
            move || read_events_from_parquet(&path)
        })
        .await
        .map_err(|e| anyhow::anyhow!("join error reading events: {}", e))?
        .map_err(|e| anyhow::anyhow!("error reading event parquet {}: {}", path.display(), e))?;

        // Filter by source (program name) when repair scope specifies sources
        let events = if let Some(scope) = repair_scope {
            if let Some(ref sources) = scope.sources {
                events
                    .into_iter()
                    .filter(|e| {
                        program_names
                            .get(&e.program_id)
                            .is_some_and(|name| sources.contains(name.as_str()))
                    })
                    .collect()
            } else {
                events
            }
        } else {
            events
        };

        // Always send the message — even when empty — so the decoder task
        // runs stale cleanup for this range. Skipping empty ranges would
        // leave old decoded parquet behind after an IDL rename/removal.
        if tx
            .send(DecoderMessage::SolanaEventsReady {
                range_start: *start,
                range_end,
                events,
                live_mode: false,
            })
            .await
            .is_err()
        {
            tracing::debug!("Event decoder channel closed early");
            return Ok(());
        }
    }

    let _ = tx.send(DecoderMessage::AllComplete).await;
    Ok(())
}

/// Read raw instruction parquet files from disk and send them as
/// [`DecoderMessage::SolanaInstructionsReady`] on the provided channel.
///
/// After all files are sent, sends [`DecoderMessage::AllComplete`] and drops
/// the sender.
async fn feed_raw_instructions_to_decoder(
    chain_name: &str,
    tx: mpsc::Sender<DecoderMessage>,
    repair_scope: Option<&RepairScope>,
    program_names: &HashMap<[u8; 32], String>,
) -> anyhow::Result<()> {
    let instructions_dir = raw_solana_instructions_dir(chain_name);

    let ranges = match scan_parquet_ranges(&instructions_dir) {
        Ok(r) => r,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            tracing::info!(
                chain = chain_name,
                "No raw instructions directory found, skipping instruction decode"
            );
            let _ = tx.send(DecoderMessage::AllComplete).await;
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };

    tracing::info!(
        chain = chain_name,
        files = ranges.len(),
        "Reading raw instruction parquet files for decode-only mode"
    );

    for (start, end_inclusive, path) in &ranges {
        let range_end = end_inclusive + 1;

        // Skip ranges outside the repair scope
        if let Some(scope) = repair_scope {
            if !scope.matches_range(*start, range_end) {
                continue;
            }
        }

        let instructions = tokio::task::spawn_blocking({
            let path = path.clone();
            move || read_instructions_from_parquet(&path)
        })
        .await
        .map_err(|e| anyhow::anyhow!("join error reading instructions: {}", e))?
        .map_err(|e| {
            anyhow::anyhow!(
                "error reading instruction parquet {}: {}",
                path.display(),
                e
            )
        })?;

        // Filter by source (program name) when repair scope specifies sources
        let instructions = if let Some(scope) = repair_scope {
            if let Some(ref sources) = scope.sources {
                instructions
                    .into_iter()
                    .filter(|i| {
                        program_names
                            .get(&i.program_id)
                            .is_some_and(|name| sources.contains(name.as_str()))
                    })
                    .collect()
            } else {
                instructions
            }
        } else {
            instructions
        };

        // Always send — see comment in feed_raw_events_to_decoder.
        if tx
            .send(DecoderMessage::SolanaInstructionsReady {
                range_start: *start,
                range_end,
                instructions,
                live_mode: false,
            })
            .await
            .is_err()
        {
            tracing::debug!("Instruction decoder channel closed early");
            return Ok(());
        }
    }

    let _ = tx.send(DecoderMessage::AllComplete).await;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::solana::discovery::{DiscoveryEventData, DiscoveryFieldValue};
    use crate::solana::live::catchup::{SlotCollectionResumeRequest, SlotCollectionResumeStage};
    use crate::types::config::eth_call::{EventTriggerConfig, EventTriggerConfigs, Frequency};
    use crate::types::config::solana::{
        SolanaAccountReadConfig, SolanaDiscoveryConfig, SolanaEventConfig, SolanaProgramConfig,
    };
    use tokio::sync::mpsc;

    fn make_programs(configs: Vec<(&str, SolanaProgramConfig)>) -> SolanaPrograms {
        configs
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect()
    }

    fn idl_program(program_id: &str) -> SolanaProgramConfig {
        SolanaProgramConfig {
            program_id: program_id.to_string(),
            idl_path: Some("/tmp/test.json".to_string()),
            idl_format: None,
            decoder: None,
            events: None,
            accounts: None,
            discovery: None,
            start_slot: None,
        }
    }

    fn builtin_program(program_id: &str, decoder: &str) -> SolanaProgramConfig {
        SolanaProgramConfig {
            program_id: program_id.to_string(),
            idl_path: None,
            idl_format: None,
            decoder: Some(decoder.to_string()),
            events: None,
            accounts: None,
            discovery: None,
            start_slot: None,
        }
    }

    fn bare_program(program_id: &str) -> SolanaProgramConfig {
        SolanaProgramConfig {
            program_id: program_id.to_string(),
            idl_path: None,
            idl_format: None,
            decoder: None,
            events: None,
            accounts: None,
            discovery: None,
            start_slot: None,
        }
    }

    #[test]
    fn features_empty_programs() {
        let features = SolanaChainFeatures::detect(&SolanaPrograms::new());
        assert!(!features.has_events);
        assert!(!features.has_instructions);
        assert!(!features.has_discovery);
        assert!(!features.has_account_reads);
    }

    #[test]
    fn features_idl_program() {
        let programs = make_programs(vec![(
            "whirlpool",
            idl_program("11111111111111111111111111111111"),
        )]);
        let features = SolanaChainFeatures::detect(&programs);
        assert!(features.has_events);
        assert!(features.has_instructions);
        assert!(!features.has_discovery);
        assert!(!features.has_account_reads);
    }

    #[test]
    fn features_builtin_decoder() {
        let programs = make_programs(vec![(
            "spl_token",
            builtin_program("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", "spl_token"),
        )]);
        let features = SolanaChainFeatures::detect(&programs);
        assert!(features.has_events);
        assert!(features.has_instructions);
    }

    #[test]
    fn features_bare_program_no_capabilities() {
        let programs = make_programs(vec![(
            "bare",
            bare_program("11111111111111111111111111111111"),
        )]);
        let features = SolanaChainFeatures::detect(&programs);
        assert!(!features.has_events);
        assert!(!features.has_instructions);
    }

    #[test]
    fn features_explicit_events_only() {
        let mut prog = bare_program("11111111111111111111111111111111");
        prog.events = Some(vec![SolanaEventConfig {
            name: "Transfer".to_string(),
            discriminator: None,
        }]);
        let programs = make_programs(vec![("test", prog)]);
        let features = SolanaChainFeatures::detect(&programs);
        assert!(features.has_events);
        assert!(!features.has_instructions);
    }

    #[test]
    fn features_discovery_detected() {
        let mut prog = idl_program("11111111111111111111111111111111");
        prog.discovery = Some(vec![SolanaDiscoveryConfig {
            event_name: "PoolCreated".to_string(),
            address_field: "pool".to_string(),
            account_type: Some("Pool".to_string()),
        }]);
        let programs = make_programs(vec![("test", prog)]);
        let features = SolanaChainFeatures::detect(&programs);
        assert!(features.has_discovery);
    }

    #[test]
    fn features_account_reads_detected() {
        let mut prog = idl_program("11111111111111111111111111111111");
        prog.accounts = Some(vec![SolanaAccountReadConfig {
            name: "pool_state".to_string(),
            account_type: "Pool".to_string(),
            frequency: crate::types::config::eth_call::Frequency::Once,
        }]);
        let programs = make_programs(vec![("test", prog)]);
        let features = SolanaChainFeatures::detect(&programs);
        assert!(features.has_account_reads);
    }

    #[test]
    fn features_on_events_account_reads_detected_without_discovery() {
        let mut prog = idl_program("11111111111111111111111111111111");
        prog.accounts = Some(vec![SolanaAccountReadConfig {
            name: "pool_state".to_string(),
            account_type: "Pool".to_string(),
            frequency: Frequency::OnEvents(EventTriggerConfigs::single(EventTriggerConfig {
                source: "emitter".to_string(),
                event: "PoolInitialized(address,address)".to_string(),
            })),
        }]);

        let programs = make_programs(vec![("test", prog)]);
        let features = SolanaChainFeatures::detect(&programs);

        assert!(features.has_account_reads);
        assert!(features.has_on_events_account_reads);
        assert!(!features.has_discovery);
    }

    #[test]
    fn live_account_completion_defers_for_discovery_once_reads_without_on_events() {
        let mut prog = idl_program("11111111111111111111111111111111");
        prog.discovery = Some(vec![SolanaDiscoveryConfig {
            event_name: "PoolCreated".to_string(),
            address_field: "pool".to_string(),
            account_type: Some("Pool".to_string()),
        }]);
        prog.accounts = Some(vec![SolanaAccountReadConfig {
            name: "pool_state".to_string(),
            account_type: "Pool".to_string(),
            frequency: crate::types::config::eth_call::Frequency::Once,
        }]);

        let programs = make_programs(vec![("test", prog)]);
        let features = SolanaChainFeatures::detect(&programs);

        assert!(build_on_events_lookup(&programs).is_empty());
        assert!(should_defer_live_account_completion(&features));
    }

    #[tokio::test]
    async fn live_account_completion_defers_for_on_events_without_discovery() {
        let mut prog = idl_program("11111111111111111111111111111111");
        prog.accounts = Some(vec![SolanaAccountReadConfig {
            name: "pool_state".to_string(),
            account_type: "Pool".to_string(),
            frequency: Frequency::OnEvents(EventTriggerConfigs::single(EventTriggerConfig {
                source: "emitter".to_string(),
                event: "PoolInitialized(address,address)".to_string(),
            })),
        }]);

        let programs = make_programs(vec![("test", prog)]);
        let features = SolanaChainFeatures::detect(&programs);

        assert!(features.has_on_events_account_reads);
        assert!(should_defer_live_account_completion(&features));
    }

    #[tokio::test]
    async fn live_decoder_destinations_route_instructions_through_discovery_bridge() {
        let (transform_tx, mut transform_rx) = mpsc::channel(1);
        let (bridge_tx, mut bridge_rx) = mpsc::channel(2);

        let (event_tx, instruction_tx) =
            build_live_decoder_destinations(true, Some(transform_tx), Some(bridge_tx));

        event_tx
            .unwrap()
            .send(DecodedEventsMessage {
                range_start: 1,
                range_end: 2,
                source_name: "program".to_string(),
                event_name: "Swap".to_string(),
                events: Vec::new(),
            })
            .await
            .unwrap();
        instruction_tx
            .unwrap()
            .send(DecodedEventsMessage {
                range_start: 1,
                range_end: 2,
                source_name: "program".to_string(),
                event_name: "Transfer".to_string(),
                events: Vec::new(),
            })
            .await
            .unwrap();

        let bridged_first = bridge_rx.recv().await.unwrap();
        let bridged_second = bridge_rx.recv().await.unwrap();

        assert_eq!(bridged_first.event_name, "Swap");
        assert_eq!(bridged_second.event_name, "Transfer");
        assert!(bridge_rx.try_recv().is_err());
        assert!(transform_rx.try_recv().is_err());
    }

    #[test]
    fn on_events_lookup_normalizes_event_signatures_to_decoded_names() {
        let mut prog = idl_program("11111111111111111111111111111111");
        prog.accounts = Some(vec![SolanaAccountReadConfig {
            name: "pool_state".to_string(),
            account_type: "Pool".to_string(),
            frequency: Frequency::OnEvents(EventTriggerConfigs::single(EventTriggerConfig {
                source: "emitter".to_string(),
                event: "PoolInitialized(address,address)".to_string(),
            })),
        }]);

        let programs = make_programs(vec![("owner_program", prog)]);
        let lookup = build_on_events_lookup(&programs);

        assert_eq!(
            lookup.get(&("emitter".to_string(), "PoolInitialized".to_string())),
            Some(&vec![OnEventAccountReadTarget {
                owner_program: "owner_program".to_string(),
                account_type: "Pool".to_string(),
            }]),
        );
        assert!(!lookup.contains_key(&(
            "emitter".to_string(),
            "PoolInitialized(address,address)".to_string(),
        )));
    }

    #[test]
    fn on_event_account_triggers_use_owning_program_addresses() {
        let mut owner_prog = idl_program("11111111111111111111111111111111");
        owner_prog.discovery = Some(vec![SolanaDiscoveryConfig {
            event_name: "PoolInitialized".to_string(),
            address_field: "pool".to_string(),
            account_type: Some("Pool".to_string()),
        }]);
        owner_prog.accounts = Some(vec![SolanaAccountReadConfig {
            name: "pool_state".to_string(),
            account_type: "Pool".to_string(),
            frequency: Frequency::OnEvents(EventTriggerConfigs::single(EventTriggerConfig {
                source: "program_b".to_string(),
                event: "PoolInitialized(address,address)".to_string(),
            })),
        }]);

        let emitter_prog = idl_program("Sysvar1111111111111111111111111111111111111");
        let programs = make_programs(vec![("program_a", owner_prog), ("program_b", emitter_prog)]);
        let lookup = build_on_events_lookup(&programs);

        let mut manager = DiscoveryManager::new(&programs);
        let known_address = [0xAB; 32];
        manager.process_events(
            "program_a",
            &[DiscoveryEventData {
                event_name: "PoolInitialized".to_string(),
                fields: HashMap::from([(
                    "pool".to_string(),
                    DiscoveryFieldValue::Pubkey(known_address),
                )]),
            }],
        );

        let triggers = collect_on_event_account_read_triggers(
            42,
            Some(1_700_000_123),
            "program_b",
            &[DiscoveryEventData {
                event_name: "PoolInitialized".to_string(),
                fields: HashMap::new(),
            }],
            &lookup,
            &manager,
            &mut HashSet::new(),
        );

        assert_eq!(triggers.len(), 1);
        assert_eq!(triggers[0].slot, 42);
        assert_eq!(triggers[0].block_time, Some(1_700_000_123));
        assert_eq!(triggers[0].source_name, "program_a");
        assert_eq!(triggers[0].addresses, vec![known_address]);
        assert!(triggers[0].await_completion_barrier);
        assert!(!triggers[0].mark_complete_only);
    }

    #[test]
    fn startup_once_account_triggers_bypass_deferred_completion_barrier() {
        let trigger = build_startup_once_account_read_trigger(
            42,
            Some(1_700_000_123),
            "program_a".to_string(),
            vec![[0xEF; 32]],
        );

        assert_eq!(trigger.slot, 42);
        assert_eq!(trigger.block_time, Some(1_700_000_123));
        assert_eq!(trigger.source_name, "program_a");
        assert_eq!(trigger.addresses, vec![[0xEF; 32]]);
        assert!(!trigger.await_completion_barrier);
        assert!(!trigger.mark_complete_only);
    }

    #[test]
    fn on_event_account_triggers_dedupe_per_slot() {
        let mut owner_prog = idl_program("11111111111111111111111111111111");
        owner_prog.discovery = Some(vec![SolanaDiscoveryConfig {
            event_name: "PoolInitialized".to_string(),
            address_field: "pool".to_string(),
            account_type: Some("Pool".to_string()),
        }]);
        owner_prog.accounts = Some(vec![SolanaAccountReadConfig {
            name: "pool_state".to_string(),
            account_type: "Pool".to_string(),
            frequency: Frequency::OnEvents(EventTriggerConfigs::single(EventTriggerConfig {
                source: "program_b".to_string(),
                event: "PoolInitialized(address,address)".to_string(),
            })),
        }]);

        let emitter_prog = idl_program("Sysvar1111111111111111111111111111111111111");
        let programs = make_programs(vec![("program_a", owner_prog), ("program_b", emitter_prog)]);
        let lookup = build_on_events_lookup(&programs);

        let mut manager = DiscoveryManager::new(&programs);
        let known_address = [0xCD; 32];
        manager.process_events(
            "program_a",
            &[DiscoveryEventData {
                event_name: "PoolInitialized".to_string(),
                fields: HashMap::from([(
                    "pool".to_string(),
                    DiscoveryFieldValue::Pubkey(known_address),
                )]),
            }],
        );

        let mut seen_targets = HashSet::new();
        let first = collect_on_event_account_read_triggers(
            42,
            Some(1_700_000_123),
            "program_b",
            &[DiscoveryEventData {
                event_name: "PoolInitialized".to_string(),
                fields: HashMap::new(),
            }],
            &lookup,
            &manager,
            &mut seen_targets,
        );
        let second = collect_on_event_account_read_triggers(
            42,
            Some(1_700_000_123),
            "program_b",
            &[DiscoveryEventData {
                event_name: "PoolInitialized".to_string(),
                fields: HashMap::new(),
            }],
            &lookup,
            &manager,
            &mut seen_targets,
        );

        assert_eq!(first.len(), 1);
        assert_eq!(second.len(), 0);
    }

    #[test]
    fn live_account_completion_does_not_defer_without_account_reads() {
        let mut prog = idl_program("11111111111111111111111111111111");
        prog.discovery = Some(vec![SolanaDiscoveryConfig {
            event_name: "PoolCreated".to_string(),
            address_field: "pool".to_string(),
            account_type: Some("Pool".to_string()),
        }]);

        let programs = make_programs(vec![("test", prog)]);
        let features = SolanaChainFeatures::detect(&programs);

        assert!(!should_defer_live_account_completion(&features));
    }

    #[test]
    fn parse_program_id_valid() {
        // 11111111111111111111111111111111 is the system program
        let bytes = parse_program_id("11111111111111111111111111111111").unwrap();
        assert_eq!(bytes, [0u8; 32]);
    }

    #[test]
    fn parse_program_id_invalid() {
        assert!(parse_program_id("not-a-valid-base58!!!").is_err());
        assert!(parse_program_id("").is_err());
    }

    #[test]
    fn build_program_names_and_configured() {
        let mut programs = SolanaPrograms::new();
        programs.insert(
            "spl_token".to_string(),
            builtin_program("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", "spl_token"),
        );

        // Construct a minimal ChainConfig to test with
        let chain = test_chain_config(programs);
        let names = build_program_names(&chain).unwrap();
        let configured = build_configured_programs(&chain).unwrap();

        assert_eq!(names.len(), 1);
        assert_eq!(configured.len(), 1);

        // The name should map back to "spl_token"
        let first_key = *configured.iter().next().unwrap();
        assert_eq!(names.get(&first_key), Some(&"spl_token".to_string()));
    }

    #[test]
    fn build_decoders_skips_bare_program_without_decoder() {
        let programs = make_programs(vec![(
            "bare",
            bare_program("11111111111111111111111111111111"),
        )]);
        let chain = test_chain_config(programs);

        let decoders = build_decoders(&chain).unwrap();
        assert!(decoders.is_empty());
    }

    #[test]
    fn build_decoders_rejects_unknown_builtin_decoder() {
        let programs = make_programs(vec![(
            "spl_token",
            builtin_program("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", "typo"),
        )]);
        let chain = test_chain_config(programs);

        let err = build_decoders(&chain)
            .err()
            .expect("expected decoder error");
        assert!(
            err.to_string().contains("Unknown built-in decoder 'typo'"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn build_decoders_rejects_events_without_decoder() {
        let mut prog = bare_program("11111111111111111111111111111111");
        prog.events = Some(vec![SolanaEventConfig {
            name: "Transfer".to_string(),
            discriminator: None,
        }]);
        let chain = test_chain_config(make_programs(vec![("test", prog)]));

        let err = build_decoders(&chain)
            .err()
            .expect("expected decoder error");
        assert!(
            err.to_string().contains("enables events"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn build_decoders_rejects_accounts_without_decoder() {
        let mut prog = bare_program("11111111111111111111111111111111");
        prog.accounts = Some(vec![SolanaAccountReadConfig {
            name: "pool_state".to_string(),
            account_type: "Pool".to_string(),
            frequency: Frequency::Once,
        }]);
        let chain = test_chain_config(make_programs(vec![("test", prog)]));

        let err = build_decoders(&chain)
            .err()
            .expect("expected decoder error");
        assert!(
            err.to_string().contains("enables accounts"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn collect_incomplete_live_slots_deduplicates_all_resume_buckets() {
        let scan = SolanaCatchupScanResult {
            slots_needing_collection_resume: vec![SlotCollectionResumeRequest {
                slot: 100,
                stage: SlotCollectionResumeStage::FetchBlock,
            }],
            slots_needing_event_decode: vec![101, 102],
            slots_needing_instruction_decode: vec![102, 103],
            slots_needing_account_reads: vec![104],
            slots_needing_transform: vec![
                (103, HashSet::from(["handler_a".to_string()])),
                (105, HashSet::from(["handler_b".to_string()])),
            ],
        };

        let slots: Vec<u64> = collect_incomplete_live_slots(&scan).into_iter().collect();
        assert_eq!(slots, vec![100, 101, 102, 103, 104, 105]);
    }

    #[test]
    fn compute_live_expected_start_slot_prefers_remaining_tip() {
        let deleted_slots = BTreeSet::from([101, 102]);
        assert_eq!(
            compute_live_expected_start_slot(Some(100), &deleted_slots),
            Some(100)
        );
    }

    #[test]
    fn compute_live_expected_start_slot_falls_back_before_deleted_tip() {
        let deleted_slots = BTreeSet::from([101, 102]);
        assert_eq!(
            compute_live_expected_start_slot(None, &deleted_slots),
            Some(100)
        );
    }

    #[test]
    fn decoded_events_block_time_uses_first_event_timestamp() {
        use crate::transformations::context::{DecodedEvent, DecodedValue};
        use crate::types::chain::{ChainAddress, LogPosition, TxId};

        let events = vec![DecodedEvent {
            block_number: 100,
            block_timestamp: 1_700_000_123,
            transaction_id: TxId::Solana([0xAA; 64]),
            position: LogPosition::Solana {
                instruction_index: 0,
                inner_instruction_index: None,
            },
            contract_address: ChainAddress::Solana([0xBB; 32]),
            source_name: "whirlpool".to_string(),
            event_name: "Swap".to_string(),
            event_signature: "Swap".to_string(),
            params: HashMap::from([(
                std::sync::Arc::<str>::from("amount"),
                DecodedValue::Uint64(1),
            )]),
        }];

        assert_eq!(decoded_events_block_time(&events), Some(1_700_000_123));
    }

    /// Build a minimal ChainConfig for testing.
    fn test_chain_config(programs: SolanaPrograms) -> ChainConfig {
        use crate::types::chain::ChainType;
        use crate::types::config::chain::RpcConfig;
        use crate::types::config::contract::{Contracts, FactoryCollections};
        use crate::types::config::solana::{SolanaCommitment, SolanaHistoricalProvider};

        ChainConfig {
            name: "test-solana".to_string(),
            chain_id: 0,
            chain_type: ChainType::Solana,
            rpc_url_env_var: "SOLANA_RPC_URL".to_string(),
            ws_url_env_var: None,
            start_block: None,
            from_block: None,
            to_block: None,
            contracts: Contracts::new(),
            block_receipts_method: None,
            factory_collections: FactoryCollections::new(),
            rpc: RpcConfig::default(),
            solana_programs: programs,
            commitment: SolanaCommitment::default(),
            historical_provider: SolanaHistoricalProvider::default(),
        }
    }

    // -----------------------------------------------------------------------
    // Tests: build_per_slot_account_types
    // -----------------------------------------------------------------------

    #[test]
    fn per_slot_types_includes_every_block() {
        let mut prog = idl_program("11111111111111111111111111111111");
        prog.accounts = Some(vec![SolanaAccountReadConfig {
            name: "pool".to_string(),
            account_type: "Pool".to_string(),
            frequency: crate::types::config::eth_call::Frequency::EveryBlock,
        }]);
        let programs = make_programs(vec![("test", prog)]);

        let result = build_per_slot_account_types(&programs);
        assert!(result["test"].contains("Pool"));
    }

    #[test]
    fn per_slot_types_excludes_once() {
        let mut prog = idl_program("11111111111111111111111111111111");
        prog.accounts = Some(vec![SolanaAccountReadConfig {
            name: "pool".to_string(),
            account_type: "Pool".to_string(),
            frequency: crate::types::config::eth_call::Frequency::Once,
        }]);
        let programs = make_programs(vec![("test", prog)]);

        let result = build_per_slot_account_types(&programs);
        // "test" should not appear at all (no per-slot types)
        assert!(
            !result.contains_key("test"),
            "Once frequency should be excluded from per-slot types"
        );
    }

    #[test]
    fn per_slot_types_mixed_frequencies() {
        let mut prog = idl_program("11111111111111111111111111111111");
        prog.accounts = Some(vec![
            SolanaAccountReadConfig {
                name: "pool".to_string(),
                account_type: "Pool".to_string(),
                frequency: crate::types::config::eth_call::Frequency::EveryBlock,
            },
            SolanaAccountReadConfig {
                name: "position".to_string(),
                account_type: "Position".to_string(),
                frequency: crate::types::config::eth_call::Frequency::Once,
            },
        ]);
        let programs = make_programs(vec![("test", prog)]);

        let result = build_per_slot_account_types(&programs);
        assert!(result["test"].contains("Pool"));
        assert!(!result["test"].contains("Position"));
    }
}
