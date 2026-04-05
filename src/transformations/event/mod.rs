//! Event handlers for transformation.
//!
//! Add new event handler modules here and register them in `register_handlers`.

pub mod decay_multicurve;
pub mod derc20_transfer;
pub mod dhook;
pub mod metrics;
pub mod multicurve;
pub mod scheduled_multicurve;
pub mod v3;
pub mod v4;

use std::sync::Arc;

use super::registry::TransformationRegistry;
use super::util::pool_metadata::PoolMetadataCache;

/// Register all event handlers with the registry.
pub fn register_handlers(registry: &mut TransformationRegistry, chain_id: u64) {
    derc20_transfer::register_handlers(registry);
    v4::create::register_handlers(registry);
    multicurve::create::register_handlers(registry);
    scheduled_multicurve::create::register_handlers(registry);
    decay_multicurve::create::register_handlers(registry);
    dhook::create::register_handlers(registry);

    // V3 Create and Metrics handlers share a PoolMetadataCache so that
    // pools created in the same range/block are visible to swap handlers
    // in-memory before the Create handler's DB transaction commits.
    let v3_cache = Arc::new(PoolMetadataCache::new());
    v3::create::register_handlers(registry, v3_cache.clone());
    v3::metrics::register_handlers(registry, chain_id, v3_cache);

    // V4 hook metrics — each pool type gets its own cache
    let multicurve_cache = Arc::new(PoolMetadataCache::new());
    multicurve::metrics::register_handlers(registry, chain_id, multicurve_cache);

    let decay_multicurve_cache = Arc::new(PoolMetadataCache::new());
    decay_multicurve::metrics::register_handlers(registry, chain_id, decay_multicurve_cache);

    let scheduled_multicurve_cache = Arc::new(PoolMetadataCache::new());
    scheduled_multicurve::metrics::register_handlers(
        registry,
        chain_id,
        scheduled_multicurve_cache,
    );

    let dhook_cache = Arc::new(PoolMetadataCache::new());
    dhook::metrics::register_handlers(registry, chain_id, dhook_cache);
}
