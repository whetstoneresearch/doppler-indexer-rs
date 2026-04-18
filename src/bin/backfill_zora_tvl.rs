//! Backfill swap metrics and TVL for Zora migrated pools.
//!
//! Reads Swapped events and getPoolCoin calls from historical decoded parquet
//! files and re-computes pool_snapshots + pool_state for pools created by
//! ZoraMigrateHandler. Automatically targets only migrated pools by scoping
//! the metadata cache to ZoraMigrateHandler — non-migrated pools are skipped.
//!
//! Does NOT update _handler_progress. Swap and TVL handlers already have
//! progress entries for historical ranges; the live indexer won't re-process
//! them. This binary just fills in the rows those handlers missed because the
//! migrated pool records didn't exist when they ran.
//!
//! Idempotent — swap metrics use ON CONFLICT DO UPDATE; TVL uses the same.
//!
//! Usage:
//!   DATABASE_URL=postgresql://... cargo run --bin backfill_zora_tvl [-- <chain>]
//!
//! Defaults to chain "base".

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;

use doppler_indexer_rs::db::DbPool;
use doppler_indexer_rs::rpc::UnifiedRpcClient;
use doppler_indexer_rs::storage::paths::{
    decoded_eth_calls_dir, decoded_logs_dir, scan_parquet_ranges,
};
use doppler_indexer_rs::transformations::context::TransformationContext;
use doppler_indexer_rs::transformations::event::metrics::swap_data::process_swaps;
use doppler_indexer_rs::transformations::event::metrics::tvl::process_tvl_from_positions;
use doppler_indexer_rs::transformations::event::zora::metrics::{
    extract_zora_swaps, extract_zora_tvl_targets_for_source,
};
use doppler_indexer_rs::transformations::historical::HistoricalDataReader;
use doppler_indexer_rs::transformations::util::pool_metadata::{PoolMetadataCache, VersionedSource};
use doppler_indexer_rs::transformations::util::usd_price::UsdPriceContext;

const CHAIN_ID: u64 = 8453;

// WETH on Base: 0x4200000000000000000000000000000000000006
const WETH: [u8; 20] = [
    0x42, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06,
];

// USDC on Base: 0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913
const USDC: [u8; 20] = [
    0x83, 0x35, 0x89, 0xfc, 0xD6, 0xeD, 0xb6, 0xE0, 0x8f, 0x4c,
    0x7C, 0x32, 0xD4, 0xf7, 0x1b, 0x54, 0xbd, 0xA0, 0x29, 0x13,
];

const SOURCES: &[&str] = &["ZoraCreatorCoinHook", "ZoraContentCoinHook"];

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let chain = std::env::args().nth(1).unwrap_or_else(|| "base".to_string());
    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://doppler:doppler@localhost/doppler".to_string());

    let db_pool = DbPool::new(&db_url, 4).await?;

    // Scope the metadata cache to ZoraMigrateHandler only. process_swaps and
    // process_tvl_from_positions both skip pools not in the cache, so this
    // automatically restricts all computation to migrated pools.
    //
    // Token scopes stay on ZoraCreateHandler because total_supply and token
    // metadata live on the original create-handler rows, not the migrate rows.
    let migrate_scope = vec![VersionedSource::new("ZoraMigrateHandler", 1)];
    let create_scope = vec![VersionedSource::new("ZoraCreateHandler", 1)];
    let metadata_cache = Arc::new(
        PoolMetadataCache::load_from_db(&db_pool, CHAIN_ID, &migrate_scope, &create_scope).await?,
    );
    eprintln!("Metadata cache loaded (ZoraMigrateHandler scope — only migrated pools)");

    let usd_ctx = UsdPriceContext::build_from_db(
        db_pool.inner(),
        CHAIN_ID,
        Some(WETH),
        &[USDC],
        None,
    )
    .await;
    eprintln!("USD price context built");

    let historical = Arc::new(HistoricalDataReader::new(&chain)?);
    let rpc = Arc::new(UnifiedRpcClient::from_url("http://localhost:8545")?);

    let mut grand_targets = 0u64;
    let mut grand_ops = 0u64;

    for source in SOURCES {
        eprintln!("Processing {source}...");
        let (n_targets, n_ops) = process_source(
            source,
            &chain,
            &historical,
            Arc::clone(&metadata_cache),
            &usd_ctx,
            &db_pool,
            Arc::clone(&rpc),
        )
        .await?;
        eprintln!("  {n_targets} targets, {n_ops} ops");
        grand_targets += n_targets;
        grand_ops += n_ops;
    }

    eprintln!("Done. Total: {grand_targets} targets, {grand_ops} ops");

    let conn = db_pool.inner().get().await?;
    let n = conn
        .execute(
            "INSERT INTO _handler_progress (chain_id, handler_key, range_start, range_end) \
             SELECT chain_id, 'ZoraMigrateHandler_v1', range_start, range_end \
             FROM _handler_progress \
             WHERE chain_id = $1 AND handler_key = 'ZoraCreateHandler_v1' \
             ON CONFLICT (chain_id, handler_key, range_start) DO UPDATE SET range_end = EXCLUDED.range_end",
            &[&(CHAIN_ID as i64)],
        )
        .await?;
    eprintln!("recorded {} ZoraMigrateHandler_v1 progress entries", n);

    Ok(())
}

async fn process_source(
    source: &str,
    chain: &str,
    historical: &Arc<HistoricalDataReader>,
    metadata_cache: Arc<PoolMetadataCache>,
    usd_ctx: &UsdPriceContext,
    db_pool: &DbPool,
    rpc: Arc<UnifiedRpcClient>,
) -> Result<(u64, u64)> {
    let events_dir = decoded_logs_dir(chain).join(source).join("Swapped");
    let calls_base = decoded_eth_calls_dir(chain).join(source).join("getPoolCoin");
    let calls_dir = if calls_base.join("on_events").is_dir() {
        calls_base.join("on_events")
    } else {
        calls_base
    };

    if !events_dir.is_dir() {
        eprintln!("  No events dir at {}, skipping", events_dir.display());
        return Ok((0, 0));
    }

    let event_files = scan_parquet_ranges(&events_dir)?;
    let call_files = if calls_dir.is_dir() {
        scan_parquet_ranges(&calls_dir)?
    } else {
        eprintln!("  Warning: no getPoolCoin dir at {}", calls_dir.display());
        vec![]
    };
    eprintln!(
        "  {} event files, {} call files",
        event_files.len(),
        call_files.len()
    );

    let mut total_targets = 0u64;
    let mut total_ops = 0u64;

    for (start, end, event_file) in &event_files {
        let events = historical.read_events_from_file(event_file, source, "Swapped")?;
        if events.is_empty() {
            continue;
        }

        let mut calls = Vec::new();
        for (call_start, call_end, call_file) in &call_files {
            if call_end < start || call_start > end {
                continue;
            }
            for call in historical.read_calls_from_file(call_file, source, "getPoolCoin")? {
                if call.block_number >= *start && call.block_number <= *end {
                    calls.push(call);
                }
            }
        }

        let ctx = TransformationContext::new(
            chain.to_string(),
            CHAIN_ID,
            *start,
            *end,
            Arc::new(events),
            Arc::new(calls),
            Arc::new(HashMap::new()),
            Arc::clone(historical),
            Arc::clone(&rpc),
            Arc::new(HashMap::new()),
        );

        let mut ops = Vec::new();

        // Swap metrics pass — metadata cache filters to migrated pools only.
        match extract_zora_swaps(&ctx) {
            Ok(swaps) if !swaps.is_empty() => {
                let swap_ops = process_swaps(
                    &swaps,
                    &metadata_cache,
                    CHAIN_ID,
                    "ZoraSwapMetricsHandler",
                    source,
                    Some(usd_ctx),
                    1,
                );
                total_targets += swap_ops.len() as u64;
                ops.extend(swap_ops);
            }
            Ok(_) => {}
            Err(e) => eprintln!("  Warning: swap extract blocks {start}-{end}: {e}"),
        }

        // TVL pass — same metadata cache, same automatic filtering.
        match extract_zora_tvl_targets_for_source(&ctx, source) {
            Ok(targets) if !targets.is_empty() => {
                total_targets += targets.len() as u64;
                match process_tvl_from_positions(
                    &targets,
                    &metadata_cache,
                    CHAIN_ID,
                    usd_ctx,
                    "ZoraSwapMetricsHandler",
                    1,
                )
                .await
                {
                    Ok(tvl_ops) => ops.extend(tvl_ops),
                    Err(e) => eprintln!("  Warning: TVL compute blocks {start}-{end}: {e}"),
                }
            }
            Ok(_) => {}
            Err(e) => eprintln!("  Warning: TVL extract blocks {start}-{end}: {e}"),
        }

        if !ops.is_empty() {
            total_ops += ops.len() as u64;
            db_pool.execute_transaction(ops).await?;
        }
    }

    Ok((total_targets, total_ops))
}
