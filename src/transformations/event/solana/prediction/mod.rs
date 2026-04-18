pub mod claims;
pub mod entries;
pub mod market;

use crate::transformations::registry::TransformationRegistry;

pub fn register_handlers(registry: &mut TransformationRegistry) {
    market::register_handlers(registry);
    entries::register_handlers(registry);
    claims::register_handlers(registry);
}
