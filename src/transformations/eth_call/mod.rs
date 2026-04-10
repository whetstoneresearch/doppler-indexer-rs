//! eth_call handlers for transformation.
//!
//! Add new eth_call handler modules here and register them in `register_handlers`.

mod price;

use super::registry::TransformationRegistry;

/// Register all eth_call handlers with the registry.
pub fn register_handlers(registry: &mut TransformationRegistry) {
    price::register_handlers(registry);
}
