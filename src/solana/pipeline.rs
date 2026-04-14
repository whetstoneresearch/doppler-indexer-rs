//! Solana pipeline orchestration.
//!
//! Wires together raw data collection, decoding, and transformation for Solana
//! chains. Parallel to the EVM pipeline functions in `main.rs`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::Context;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinSet;

use crate::db::DbPool;
use crate::decoding::DecoderMessage;
use crate::live::LiveProgressTracker;
use crate::solana::decoding::events::SolanaEventDecoder;
use crate::solana::decoding::idl::AnchorDecoder;
use crate::solana::decoding::instructions::SolanaInstructionDecoder;
use crate::solana::decoding::spl_token::SplTokenDecoder;
use crate::solana::decoding::tasks::{decode_solana_events, decode_solana_instructions};
use crate::solana::decoding::traits::ProgramDecoder;
use crate::solana::raw_data::catchup::signature_driven_backfill;
use crate::solana::raw_data::parquet::{read_events_from_parquet, read_instructions_from_parquet};
use crate::solana::rpc::SolanaRpcClient;
use crate::storage::paths::{
    raw_solana_events_dir, raw_solana_instructions_dir, scan_parquet_ranges,
};
use crate::transformations::registry::{build_registry_for_solana_chain, TransformationRegistry};
use crate::types::config::chain::ChainConfig;
use crate::types::config::defaults;
use crate::types::config::indexer::IndexerConfig;
use crate::types::config::solana::SolanaPrograms;
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
    pub has_account_reads: bool,
}

impl SolanaChainFeatures {
    pub fn detect(programs: &SolanaPrograms) -> Self {
        let mut has_events = false;
        let mut has_instructions = false;
        let mut has_discovery = false;
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
            }
        }

        Self {
            has_events,
            has_instructions,
            has_discovery,
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
    ) -> anyhow::Result<Self> {
        let chain = Arc::new(chain.clone());

        // Build RPC client
        let rpc_url = std::env::var(&chain.rpc_url_env_var).with_context(|| {
            format!("env var {} not set for Solana RPC", chain.rpc_url_env_var)
        })?;
        let rpc_client = Arc::new(SolanaRpcClient::new(
            &rpc_url,
            chain.commitment,
            None,
            None,
        )?);

        // Detect features
        let features = SolanaChainFeatures::detect(&chain.solana_programs);

        // Build decoders
        let decoders = build_decoders(&chain)?;

        // Build program_id -> name mapping and configured program set
        let program_names = Arc::new(build_program_names(&chain)?);
        let configured_programs = build_configured_programs(&chain)?;

        // Build registry
        let registry = Arc::new(build_registry_for_solana_chain(
            chain.chain_id,
            &chain.solana_programs,
        ));

        // Transformations enabled if registry has handlers AND db config present
        let transformations_enabled =
            registry.handler_count() > 0 && config.transformations.is_some();

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

        // Create progress tracker
        let progress_tracker = if transformations_enabled {
            let tracker = Arc::new(Mutex::new(LiveProgressTracker::new(
                chain.chain_id as i64,
                db_pool.clone(),
                chain.name.clone(),
            )));
            {
                let mut t = tracker.lock().await;
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
                    tracing::warn!(
                        program = name.as_str(),
                        decoder = other,
                        "Unknown built-in decoder, skipping"
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

        tracing::warn!(
            program = name.as_str(),
            "No decoder or IDL configured, program events/instructions will not be decoded"
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
) -> anyhow::Result<()> {
    tracing::info!(
        chain = chain.name.as_str(),
        repair,
        ?repair_scope,
        "Starting Solana pipeline"
    );

    let runtime = SolanaChainRuntime::build(config, chain, shared_db_pool).await?;

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

    // Transformation channels (decoders → engine)
    // Currently unused since no Solana handlers exist yet, but wired up
    // so the decoder tasks have somewhere to send when handlers are added.
    let (transform_events_tx, _transform_events_rx) = if runtime.transformations_enabled {
        let (tx, rx) = mpsc::channel(channel_capacity);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    let (transform_complete_tx, _transform_complete_rx) = if runtime.transformations_enabled {
        let (tx, rx) = mpsc::channel(channel_capacity);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

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

    // Scoped repair: source or function filters narrow the decoded output,
    // so stale-file cleanup must be skipped (the decoder only sees a subset).
    let scoped_repair = repair
        && repair_scope
            .as_ref()
            .is_some_and(|s| s.sources.is_some() || s.functions.is_some());

    // Backfill task
    {
        let chain_name = runtime.chain.name.clone();
        let rpc_client = runtime.rpc_client.clone();
        let programs = runtime.chain.solana_programs.clone();
        let configured_programs = runtime.configured_programs.clone();
        let event_tx = event_decoder_tx.clone();
        let instr_tx = instr_decoder_tx.clone();
        let backfill_repair_scope = if repair { repair_scope } else { None };

        tasks.spawn(async move {
            let result = signature_driven_backfill(
                &chain_name,
                &rpc_client,
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

    // Event decoder task
    if let Some(event_rx) = event_decoder_rx {
        let event_decoder = Arc::new(SolanaEventDecoder::new(runtime.decoders.clone()));
        let program_names = runtime.program_names.clone();
        let tx = transform_events_tx.clone();
        let complete = transform_complete_tx.clone();
        let chain_name = runtime.chain.name.clone();
        let ff = function_filter.clone();

        let sr = scoped_repair;
        tasks.spawn(async move {
            decode_solana_events(event_decoder, program_names, event_rx, tx, complete, chain_name, ff, sr)
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
        let sr = scoped_repair;

        tasks.spawn(async move {
            decode_solana_instructions(
                instr_decoder,
                program_names,
                instr_rx,
                tx,
                complete,
                chain_name,
                ff,
                sr,
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

    // TODO: Spawn TransformationEngine when Solana handlers exist.
    // Currently `transformations_enabled` is always false (no handlers registered).
    // When handlers are added, the engine will need to accept a Solana RPC client
    // (or make the EVM client optional). Wire _transform_events_rx and
    // _transform_complete_rx into engine.run() at that point.

    // --- Wait for all tasks ---

    while let Some(result) = tasks.join_next().await {
        result.context("Solana pipeline task panicked")??;
    }

    tracing::info!(chain = chain.name.as_str(), "Solana historical pipeline complete");

    // --- Live mode stub ---

    if !catch_up_only {
        let live_mode = config
            .raw_data_collection
            .live_mode
            .unwrap_or(false);
        if live_mode {
            tracing::warn!(
                chain = chain.name.as_str(),
                "Solana live mode not yet implemented — skipping. \
                 Live collector, reorg detection, and account reader are Phase 8."
            );
        }
    }

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

    // Scoped decode-only: source or function filters narrow the output
    let scoped = repair_scope
        .as_ref()
        .is_some_and(|s| s.sources.is_some() || s.functions.is_some());

    let mut tasks: JoinSet<anyhow::Result<()>> = JoinSet::new();

    // Event decoding
    if !decoders.is_empty() {
        let event_decoder = Arc::new(SolanaEventDecoder::new(decoders.clone()));
        let (tx, rx) = mpsc::channel::<DecoderMessage>(channel_capacity);
        let names = program_names.clone();
        let chain_name = chain.name.clone();
        let ff = function_filter.clone();

        tasks.spawn(async move {
            decode_solana_events(event_decoder, names, rx, None, None, chain_name, ff, scoped).await;
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

        tasks.spawn(async move {
            decode_solana_instructions(instr_decoder, names, rx, None, None, chain_name, ff, scoped).await;
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

        if events.is_empty() {
            continue;
        }

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

        if instructions.is_empty() {
            continue;
        }

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
    use crate::types::config::solana::{
        SolanaAccountReadConfig, SolanaDiscoveryConfig, SolanaEventConfig, SolanaProgramConfig,
    };

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
        let programs = make_programs(vec![("whirlpool", idl_program("11111111111111111111111111111111"))]);
        let features = SolanaChainFeatures::detect(&programs);
        assert!(features.has_events);
        assert!(features.has_instructions);
        assert!(!features.has_discovery);
        assert!(!features.has_account_reads);
    }

    #[test]
    fn features_builtin_decoder() {
        let programs = make_programs(vec![("spl_token", builtin_program("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", "spl_token"))]);
        let features = SolanaChainFeatures::detect(&programs);
        assert!(features.has_events);
        assert!(features.has_instructions);
    }

    #[test]
    fn features_bare_program_no_capabilities() {
        let programs = make_programs(vec![("bare", bare_program("11111111111111111111111111111111"))]);
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

    /// Build a minimal ChainConfig for testing.
    fn test_chain_config(programs: SolanaPrograms) -> ChainConfig {
        use crate::types::chain::ChainType;
        use crate::types::config::contract::{Contracts, FactoryCollections};
        use crate::types::config::chain::RpcConfig;
        use crate::types::config::solana::SolanaCommitment;

        ChainConfig {
            name: "test-solana".to_string(),
            chain_id: 0,
            chain_type: ChainType::Solana,
            rpc_url_env_var: "SOLANA_RPC_URL".to_string(),
            ws_url_env_var: None,
            start_block: None,
            contracts: Contracts::new(),
            block_receipts_method: None,
            factory_collections: FactoryCollections::new(),
            rpc: RpcConfig::default(),
            solana_programs: programs,
            commitment: SolanaCommitment::default(),
        }
    }
}
