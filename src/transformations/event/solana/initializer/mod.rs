pub mod curve_swap;
pub mod launch_create;
pub mod launch_migrate;

use crate::transformations::registry::TransformationRegistry;

pub fn register_handlers(registry: &mut TransformationRegistry) {
    launch_create::register_handlers(registry);
    curve_swap::register_handlers(registry);
    launch_migrate::register_handlers(registry);
}
