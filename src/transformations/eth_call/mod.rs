//! eth_call handlers for transformation.
//!
//! Add new eth_call handler modules here and register them in `register_handlers`.

use super::registry::TransformationRegistry;

/// Register all eth_call handlers with the registry.
pub fn register_handlers(registry: &mut TransformationRegistry) {
    // Add handler registrations here as they are implemented:
    // oracle::register_handlers(registry);
    // pool_state::register_handlers(registry);

    let _ = registry; // Suppress unused warning when no handlers are registered
}
