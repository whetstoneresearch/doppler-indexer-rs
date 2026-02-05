//! Handler registration system.
//!
//! The registry maintains a mapping from event/call triggers to their handlers.

use std::collections::HashMap;
use std::sync::Arc;

use super::traits::{EthCallHandler, EthCallTrigger, EventHandler, EventTrigger, TransformationHandler};

/// Registry of all transformation handlers, built at startup.
pub struct TransformationRegistry {
    /// Event handlers indexed by (source, event_signature) for fast lookup
    event_handlers: HashMap<(String, String), Vec<Arc<dyn EventHandler>>>,
    /// Call handlers indexed by (source, function_name)
    call_handlers: HashMap<(String, String), Vec<Arc<dyn EthCallHandler>>>,
    /// All handlers for initialization (de-duplicated)
    all_handlers: Vec<Arc<dyn TransformationHandler>>,
}

impl TransformationRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            event_handlers: HashMap::new(),
            call_handlers: HashMap::new(),
            all_handlers: Vec::new(),
        }
    }

    /// Register an event handler.
    ///
    /// The handler will be invoked for all events matching its triggers.
    pub fn register_event_handler<H: EventHandler + 'static>(&mut self, handler: H) {
        let handler = Arc::new(handler);
        let triggers = handler.triggers();

        for trigger in triggers {
            let key = (trigger.source.clone(), extract_event_name(&trigger.event_signature));
            self.event_handlers
                .entry(key)
                .or_default()
                .push(handler.clone());
        }

        self.all_handlers.push(handler);
    }

    /// Register an eth_call handler.
    ///
    /// The handler will be invoked for all call results matching its triggers.
    pub fn register_call_handler<H: EthCallHandler + 'static>(&mut self, handler: H) {
        let handler = Arc::new(handler);
        let triggers = handler.triggers();

        for trigger in triggers {
            let key = (trigger.source.clone(), trigger.function_name.clone());
            self.call_handlers
                .entry(key)
                .or_default()
                .push(handler.clone());
        }

        self.all_handlers.push(handler);
    }

    /// Get handlers for a specific event.
    pub fn handlers_for_event(
        &self,
        source: &str,
        event_name: &str,
    ) -> Vec<Arc<dyn EventHandler>> {
        let key = (source.to_string(), event_name.to_string());
        self.event_handlers.get(&key).cloned().unwrap_or_default()
    }

    /// Get handlers for a specific call.
    pub fn handlers_for_call(
        &self,
        source: &str,
        function_name: &str,
    ) -> Vec<Arc<dyn EthCallHandler>> {
        let key = (source.to_string(), function_name.to_string());
        self.call_handlers.get(&key).cloned().unwrap_or_default()
    }

    /// Get all registered event triggers.
    pub fn all_event_triggers(&self) -> Vec<(String, String)> {
        self.event_handlers.keys().cloned().collect()
    }

    /// Get all registered call triggers.
    pub fn all_call_triggers(&self) -> Vec<(String, String)> {
        self.call_handlers.keys().cloned().collect()
    }

    /// Get all handlers for initialization.
    pub fn all_handlers(&self) -> &[Arc<dyn TransformationHandler>] {
        &self.all_handlers
    }

    /// Check if any handlers are registered.
    pub fn is_empty(&self) -> bool {
        self.all_handlers.is_empty()
    }

    /// Get count of registered handlers.
    pub fn handler_count(&self) -> usize {
        self.all_handlers.len()
    }
}

impl Default for TransformationRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Extract event name from signature.
/// e.g., "Swap(bytes32,address,int128)" -> "Swap"
fn extract_event_name(signature: &str) -> String {
    signature
        .split('(')
        .next()
        .unwrap_or(signature)
        .to_string()
}

/// Build the transformation registry with all handlers.
///
/// This is where handlers are registered at compile-time.
/// Add new handler registrations here as they are implemented.
pub fn build_registry() -> TransformationRegistry {
    let mut registry = TransformationRegistry::new();

    // Register event handlers
    super::event::register_handlers(&mut registry);

    // Register eth_call handlers
    super::eth_call::register_handlers(&mut registry);

    tracing::info!(
        "Built transformation registry with {} handlers ({} event triggers, {} call triggers)",
        registry.handler_count(),
        registry.all_event_triggers().len(),
        registry.all_call_triggers().len()
    );

    registry
}
