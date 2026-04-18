pub mod metrics;
pub mod pool_create;
pub mod positions;
pub mod swap;

use crate::transformations::registry::TransformationRegistry;

pub fn register_handlers(registry: &mut TransformationRegistry) {
    pool_create::register_handlers(registry);
    swap::register_handlers(registry);
    metrics::register_handlers(registry);
    positions::register_handlers(registry);
}
