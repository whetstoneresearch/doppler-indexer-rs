//! Event handlers for transformation.
//!
//! Add new event handler modules here and register them in `register_handlers`.

pub mod v3;

use super::registry::TransformationRegistry;

/// Register all event handlers with the registry.
pub fn register_handlers(registry: &mut TransformationRegistry) {
    v3::register_handlers(registry);
    // Add more handler registrations here as they are implemented:
    // transfers::register_handlers(registry);
}
