//! Handler registration system.
//!
//! The registry maintains a mapping from event/call triggers to their handlers.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::traits::{
    EthCallHandler, EthCallTrigger, EventHandler, EventTrigger, TransformationHandler,
};
use crate::raw_data::historical::eth_calls::{
    build_call_configs, build_event_triggered_call_configs, build_factory_once_call_configs,
    build_once_call_configs,
};
use crate::raw_data::historical::factories::get_factory_call_configs;
use crate::types::config::contract::{Contracts, FactoryCollections};

/// Generic helper to deduplicate handlers by their `handler_key()`.
///
/// Takes an iterator of (handler, triggers) pairs and returns a Vec of unique HandlerInfo
/// structs, where each handler appears exactly once (keyed by `handler_key()`).
fn deduplicate_handlers<H, T, I, F>(handlers: I, get_triggers: F) -> Vec<(Arc<H>, Vec<T>)>
where
    H: TransformationHandler + ?Sized,
    T: Clone,
    I: IntoIterator<Item = Arc<H>>,
    F: Fn(&H) -> Vec<T>,
{
    let mut seen: HashMap<String, (Arc<H>, Vec<T>)> = HashMap::new();
    for handler in handlers {
        let key = handler.handler_key();
        seen.entry(key).or_insert_with(|| {
            let triggers = get_triggers(&*handler);
            (handler, triggers)
        });
    }
    seen.into_values().collect()
}

/// An event handler with its associated trigger information.
pub struct EventHandlerInfo {
    pub handler: Arc<dyn EventHandler>,
    pub triggers: Vec<EventTrigger>,
}

/// A call handler with its associated trigger information.
pub struct CallHandlerInfo {
    pub handler: Arc<dyn EthCallHandler>,
    pub triggers: Vec<EthCallTrigger>,
}

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
            let key = (
                trigger.source.clone(),
                extract_event_name(&trigger.event_signature),
            );
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
    #[allow(dead_code)]
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
    pub fn handlers_for_event(&self, source: &str, event_name: &str) -> Vec<Arc<dyn EventHandler>> {
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

    /// Get all unique event handlers with their triggers grouped.
    /// Each handler appears exactly once, deduplicated by `handler_key()`.
    pub fn unique_event_handlers(&self) -> Vec<EventHandlerInfo> {
        deduplicate_handlers(self.event_handlers.values().flatten().cloned(), |h| {
            h.triggers()
        })
        .into_iter()
        .map(|(handler, triggers)| EventHandlerInfo { handler, triggers })
        .collect()
    }

    /// Get all unique call handlers with their triggers grouped.
    /// Each handler appears exactly once, deduplicated by `handler_key()`.
    pub fn unique_call_handlers(&self) -> Vec<CallHandlerInfo> {
        deduplicate_handlers(self.call_handlers.values().flatten().cloned(), |h| {
            h.triggers()
        })
        .into_iter()
        .map(|(handler, triggers)| CallHandlerInfo { handler, triggers })
        .collect()
    }
}

impl Default for TransformationRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Extract event name from signature.
/// e.g., "Swap(bytes32,address,int128)" -> "Swap"
pub fn extract_event_name(signature: &str) -> String {
    signature.split('(').next().unwrap_or(signature).to_string()
}

/// Validate that all handler call_dependencies are satisfied by the eth_call configs.
///
/// Panics at startup with a descriptive message if any handler declares a
/// `call_dependencies()` entry that is not configured for eth_call collection.
pub fn validate_call_dependencies(
    registry: &TransformationRegistry,
    contracts: &Contracts,
    factory_collections: &FactoryCollections,
) {
    let mut available: HashSet<(String, String)> = HashSet::new();

    // Regular periodic calls: (contract_name, function_name)
    if let Ok(call_configs) = build_call_configs(contracts) {
        for config in &call_configs {
            available.insert((config.contract_name.clone(), config.function_name.clone()));
        }
    }

    // Once calls: (contract_name, "once")
    let once_configs = build_once_call_configs(contracts);
    for contract_name in once_configs.keys() {
        available.insert((contract_name.clone(), "once".to_string()));
    }

    // Factory call configs (needed for both factory periodic and factory once)
    let factory_call_configs = get_factory_call_configs(contracts, factory_collections);

    // Factory once calls: (collection_name, "once")
    let factory_once_configs = build_factory_once_call_configs(&factory_call_configs, contracts);
    for collection_name in factory_once_configs.keys() {
        available.insert((collection_name.clone(), "once".to_string()));
    }

    // Event-triggered calls: (contract_name, function_name)
    let event_triggered_configs = build_event_triggered_call_configs(contracts);
    for configs in event_triggered_configs.values() {
        for config in configs {
            available.insert((config.contract_name.clone(), config.function_name.clone()));
        }
    }

    // Factory periodic calls: (collection_name, function_name)
    for (collection_name, configs) in &factory_call_configs {
        for call in configs {
            let function_name = call
                .function
                .split('(')
                .next()
                .unwrap_or(&call.function)
                .to_string();
            available.insert((collection_name.clone(), function_name));
        }
    }

    // Validate each handler's call_dependencies
    let mut all_missing: Vec<(String, Vec<(String, String)>)> = Vec::new();

    for handler_info in registry.unique_event_handlers() {
        let deps = handler_info.handler.call_dependencies();
        if deps.is_empty() {
            continue;
        }

        let missing: Vec<(String, String)> = deps
            .into_iter()
            .filter(|dep| !available.contains(dep))
            .collect();

        if !missing.is_empty() {
            all_missing.push((handler_info.handler.handler_key(), missing));
        }
    }

    if !all_missing.is_empty() {
        let mut msg = String::from(
            "missing call dependency: one or more handlers declare call_dependencies \
             that are not configured for eth_call collection:\n",
        );
        for (handler_key, missing) in &all_missing {
            msg.push_str(&format!("\n  Handler '{}':\n", handler_key));
            for (source, function) in missing {
                msg.push_str(&format!("    - ({}, {})\n", source, function));
            }
        }
        msg.push_str("\nAvailable call configs:\n");
        let mut sorted_available: Vec<_> = available.iter().collect();
        sorted_available.sort();
        for (source, function) in &sorted_available {
            msg.push_str(&format!("    - ({}, {})\n", source, function));
        }
        msg.push_str("\nCheck your config files to ensure all required eth_calls are configured.");
        panic!("{}", msg);
    }
}

/// Build the transformation registry with all handlers.
///
/// This is where handlers are registered at compile-time.
/// Add new handler registrations here as they are implemented.
pub fn build_registry(chain_id: u64) -> TransformationRegistry {
    let mut registry = TransformationRegistry::new();

    // Register event handlers
    super::event::register_handlers(&mut registry, chain_id);

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbOperation;
    use crate::transformations::context::TransformationContext;
    use crate::transformations::error::TransformationError;
    use crate::transformations::traits::{EventHandler, EventTrigger, TransformationHandler};
    use async_trait::async_trait;

    /// A mock event handler for testing call dependency validation.
    struct MockEventHandler {
        name: &'static str,
        triggers: Vec<EventTrigger>,
        call_deps: Vec<(String, String)>,
    }

    #[async_trait]
    impl TransformationHandler for MockEventHandler {
        fn name(&self) -> &'static str {
            self.name
        }

        async fn handle(
            &self,
            _ctx: &TransformationContext,
        ) -> Result<Vec<DbOperation>, TransformationError> {
            Ok(vec![])
        }
    }

    impl EventHandler for MockEventHandler {
        fn triggers(&self) -> Vec<EventTrigger> {
            self.triggers.clone()
        }

        fn call_dependencies(&self) -> Vec<(String, String)> {
            self.call_deps.clone()
        }
    }

    fn empty_contracts() -> Contracts {
        Contracts::new()
    }

    fn empty_factory_collections() -> FactoryCollections {
        FactoryCollections::new()
    }

    #[test]
    fn validate_empty_registry_passes() {
        let registry = TransformationRegistry::new();
        let contracts = empty_contracts();
        let factory_collections = empty_factory_collections();

        // Should not panic
        validate_call_dependencies(&registry, &contracts, &factory_collections);
    }

    #[test]
    fn validate_handler_with_no_deps_passes() {
        let mut registry = TransformationRegistry::new();
        registry.register_event_handler(MockEventHandler {
            name: "test_handler",
            triggers: vec![EventTrigger::new(
                "TestContract",
                "Transfer(address,address,uint256)",
            )],
            call_deps: vec![],
        });

        let contracts = empty_contracts();
        let factory_collections = empty_factory_collections();

        // Should not panic — no dependencies declared
        validate_call_dependencies(&registry, &contracts, &factory_collections);
    }

    #[test]
    fn validate_handler_with_satisfied_periodic_dep_passes() {
        use crate::types::config::contract::AddressOrAddresses;
        use crate::types::config::contract::ContractConfig;
        use crate::types::config::eth_call::{EthCallConfig, EvmType, Frequency};
        use alloy_primitives::Address;

        let mut registry = TransformationRegistry::new();
        registry.register_event_handler(MockEventHandler {
            name: "test_handler",
            triggers: vec![EventTrigger::new("TestContract", "Swap(address,uint256)")],
            call_deps: vec![("TestContract".to_string(), "getState".to_string())],
        });

        let mut contracts = Contracts::new();
        contracts.insert(
            "TestContract".to_string(),
            ContractConfig {
                address: AddressOrAddresses::Single(Address::ZERO),
                start_block: None,
                calls: Some(vec![EthCallConfig {
                    function: "getState(address)".to_string(),
                    output_type: EvmType::Uint256,
                    params: vec![],
                    frequency: Frequency::default(),
                    target: None,
                }]),
                factories: None,
                events: None,
            },
        );

        let factory_collections = empty_factory_collections();

        // Should not panic — dependency is satisfied by periodic call config
        validate_call_dependencies(&registry, &contracts, &factory_collections);
    }

    #[test]
    #[should_panic(expected = "missing call dependency")]
    fn validate_handler_with_missing_dep_panics() {
        let mut registry = TransformationRegistry::new();
        registry.register_event_handler(MockEventHandler {
            name: "test_handler",
            triggers: vec![EventTrigger::new("TestContract", "Swap(address,uint256)")],
            call_deps: vec![("MissingContract".to_string(), "getState".to_string())],
        });

        let contracts = empty_contracts();
        let factory_collections = empty_factory_collections();

        // Should panic — dependency is not configured
        validate_call_dependencies(&registry, &contracts, &factory_collections);
    }

    #[test]
    #[should_panic(expected = "missing call dependency")]
    fn validate_handler_with_wrong_function_name_panics() {
        use crate::types::config::contract::AddressOrAddresses;
        use crate::types::config::contract::ContractConfig;
        use crate::types::config::eth_call::{EthCallConfig, EvmType, Frequency};
        use alloy_primitives::Address;

        let mut registry = TransformationRegistry::new();
        registry.register_event_handler(MockEventHandler {
            name: "test_handler",
            triggers: vec![EventTrigger::new("TestContract", "Swap(address,uint256)")],
            call_deps: vec![("TestContract".to_string(), "wrongFunction".to_string())],
        });

        let mut contracts = Contracts::new();
        contracts.insert(
            "TestContract".to_string(),
            ContractConfig {
                address: AddressOrAddresses::Single(Address::ZERO),
                start_block: None,
                calls: Some(vec![EthCallConfig {
                    function: "getState(address)".to_string(),
                    output_type: EvmType::Uint256,
                    params: vec![],
                    frequency: Frequency::default(),
                    target: None,
                }]),
                factories: None,
                events: None,
            },
        );

        let factory_collections = empty_factory_collections();

        // Should panic — function name doesn't match
        validate_call_dependencies(&registry, &contracts, &factory_collections);
    }

    #[test]
    fn validate_handler_with_once_dep_passes() {
        use crate::types::config::contract::AddressOrAddresses;
        use crate::types::config::contract::ContractConfig;
        use crate::types::config::eth_call::{EthCallConfig, EvmType, Frequency};
        use alloy_primitives::Address;

        let mut registry = TransformationRegistry::new();
        registry.register_event_handler(MockEventHandler {
            name: "test_handler",
            triggers: vec![EventTrigger::new("TestContract", "Swap(address,uint256)")],
            call_deps: vec![("TestContract".to_string(), "once".to_string())],
        });

        let mut contracts = Contracts::new();
        contracts.insert(
            "TestContract".to_string(),
            ContractConfig {
                address: AddressOrAddresses::Single(Address::ZERO),
                start_block: None,
                calls: Some(vec![EthCallConfig {
                    function: "decimals()".to_string(),
                    output_type: EvmType::Uint256,
                    params: vec![],
                    frequency: Frequency::Once,
                    target: None,
                }]),
                factories: None,
                events: None,
            },
        );

        let factory_collections = empty_factory_collections();

        // Should not panic — once dependency is satisfied
        validate_call_dependencies(&registry, &contracts, &factory_collections);
    }
}
