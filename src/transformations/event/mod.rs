//! Event handlers for transformation.
//!
//! Add new event handler modules here and register them in `register_handlers`.

pub mod decay_multicurve;
pub mod derc20_transfer;
pub mod dhook;
pub mod metrics;
pub mod migration_pool;
pub mod multicurve;
pub mod scheduled_multicurve;
pub mod v3;
pub mod v4;
pub mod zora;

use std::sync::Arc;

use super::registry::TransformationRegistry;
use super::util::pool_metadata::PoolMetadataCache;
use super::util::usd_price::OraclePriceCache;
use crate::types::config::contract::Contracts;

/// Register all event handlers with the registry.
pub fn register_handlers(registry: &mut TransformationRegistry, chain_id: u64) {
    register_handlers_inner(registry, chain_id, None);
}

/// Register event handlers filtered by the chain's configured contracts.
pub fn register_handlers_for_chain(
    registry: &mut TransformationRegistry,
    chain_id: u64,
    contracts: &Contracts,
) {
    register_handlers_inner(registry, chain_id, Some(contracts));
}

fn register_handlers_inner(
    registry: &mut TransformationRegistry,
    chain_id: u64,
    contracts: Option<&Contracts>,
) {
    // derc20_transfer::register_handlers(registry);
    v4::create::register_handlers(registry);
    multicurve::create::register_handlers(registry);
    scheduled_multicurve::create::register_handlers(registry);
    //decay_multicurve::create::register_handlers(registry);
    //dhook::create::register_handlers(registry);
    zora::create::register_handlers(registry);
    //zora::transfer::register_handlers(registry);

    // Shared oracle price cache across all swap metrics handlers.
    // ETH/USD and EURC/USD prices are shared so a single oracle reading
    // bridges the 15m gap across all handler types.
    let oracle_cache = Arc::new(match contracts {
        Some(contracts) => OraclePriceCache::with_contracts(contracts),
        None => OraclePriceCache::new(),
    });

    // V3 Create and Metrics handlers share a PoolMetadataCache so that
    // pools created in the same range/block are visible to swap handlers
    // in-memory before the Create handler's DB transaction commits.
    let v3_cache = Arc::new(PoolMetadataCache::with_shared_scopes(vec![
        v3::create::V3_CREATE_HANDLER_SCOPE,
        v3::create::LOCKABLE_V3_CREATE_HANDLER_SCOPE,
    ]));
    v3::create::register_handlers(registry, v3_cache.clone());
    v3::metrics::register_handlers(registry, chain_id, v3_cache, Arc::clone(&oracle_cache));

    // V4 hook metrics — each pool type gets its own metadata cache
    let multicurve_cache = Arc::new(PoolMetadataCache::with_shared_scopes(vec![
        multicurve::create::V4_MULTICURVE_CREATE_HANDLER_SCOPE,
    ]));
    multicurve::metrics::register_handlers(
        registry,
        chain_id,
        multicurve_cache,
        Arc::clone(&oracle_cache),
    );

    // let decay_multicurve_cache = Arc::new(PoolMetadataCache::with_shared_scopes(vec![
    //     decay_multicurve::create::V4_DECAY_MULTICURVE_CREATE_HANDLER_SCOPE,
    // ]));
    // decay_multicurve::metrics::register_handlers(
    //     registry,
    //     chain_id,
    //     decay_multicurve_cache,
    //     Arc::clone(&oracle_cache),
    // );

    let scheduled_multicurve_cache = Arc::new(PoolMetadataCache::with_shared_scopes(vec![
        scheduled_multicurve::create::V4_SCHEDULED_MULTICURVE_CREATE_HANDLER_SCOPE,
    ]));
    scheduled_multicurve::metrics::register_handlers(
        registry,
        chain_id,
        scheduled_multicurve_cache,
        Arc::clone(&oracle_cache),
    );

    // let dhook_cache = Arc::new(PoolMetadataCache::with_shared_scopes(vec![
    //     dhook::create::DOPPLER_HOOK_CREATE_HANDLER_SCOPE,
    // ]));
    // dhook::metrics::register_handlers(registry, chain_id, dhook_cache, Arc::clone(&oracle_cache));

    // V4 base (DopplerV4Hook) — sequential handler, own metadata cache
    let v4_base_cache = Arc::new(PoolMetadataCache::with_shared_scopes(vec![
        v4::create::V4_CREATE_HANDLER_SCOPE,
    ]));
    v4::metrics::register_handlers(registry, chain_id, v4_base_cache, Arc::clone(&oracle_cache));

    let zora_cache = Arc::new(PoolMetadataCache::with_shared_scopes(vec![
        zora::create::ZORA_CREATE_HANDLER_SCOPE,
    ]));
    zora::metrics::register_handlers(registry, chain_id, zora_cache, Arc::clone(&oracle_cache));

    // Register migration pool handlers as a group only on chains with a V4
    // migrator. The swap handler depends on MigrationPoolCreateHandler, so
    // letting source filtering split them apart can leave a dangling dependency
    // on PoolManager-only chains.
    // let register_migration_pool_handlers = contracts
    //     .map(|contracts| contracts.contains_key("UniswapV4Migrator"))
    //     .unwrap_or(true);
    // if register_migration_pool_handlers {
    //     migration_pool::create::register_handlers(registry);
    //     let migration_pool_cache = Arc::new(PoolMetadataCache::with_scopes(
    //         vec![migration_pool::create::MIGRATION_POOL_CREATE_HANDLER_SCOPE],
    //         vec![
    //             v3::create::V3_CREATE_HANDLER_SCOPE,
    //             v3::create::LOCKABLE_V3_CREATE_HANDLER_SCOPE,
    //             v4::create::V4_CREATE_HANDLER_SCOPE,
    //             multicurve::create::V4_MULTICURVE_CREATE_HANDLER_SCOPE,
    //             decay_multicurve::create::V4_DECAY_MULTICURVE_CREATE_HANDLER_SCOPE,
    //             scheduled_multicurve::create::V4_SCHEDULED_MULTICURVE_CREATE_HANDLER_SCOPE,
    //             dhook::create::DOPPLER_HOOK_CREATE_HANDLER_SCOPE,
    //         ],
    //     ));
    //     migration_pool::metrics::register_handlers(
    //         registry,
    //         chain_id,
    //         migration_pool_cache,
    //         Arc::clone(&oracle_cache),
    //     );
    //}
}
