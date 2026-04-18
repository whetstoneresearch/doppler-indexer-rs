pub mod handlers;

use crate::transformations::registry::TransformationRegistry;

pub fn register_handlers(registry: &mut TransformationRegistry) {
    handlers::register_handlers(registry);
}
