//! Handler registration system.
//!
//! The registry maintains a mapping from event/call triggers to their handlers.

use std::collections::{HashMap, HashSet, VecDeque};
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
    /// Maps handler name() to its declared handler dependency names
    handler_dependency_graph: HashMap<String, Vec<String>>,
    /// Topological ordering of handler names (computed after all handlers registered)
    handler_topological_order: Vec<String>,
}

impl TransformationRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            event_handlers: HashMap::new(),
            call_handlers: HashMap::new(),
            all_handlers: Vec::new(),
            handler_dependency_graph: HashMap::new(),
            handler_topological_order: Vec::new(),
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

        let deps: Vec<String> = handler
            .handler_dependencies()
            .iter()
            .map(|d| d.to_string())
            .collect();
        if !deps.is_empty() {
            self.handler_dependency_graph
                .insert(handler.name().to_string(), deps);
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

    /// Get the topological ordering of handler names.
    /// Available after `validate_and_sort_handler_dependencies()` has been called.
    pub fn handler_topological_order(&self) -> &[String] {
        &self.handler_topological_order
    }

    /// Get the dependency graph (handler name -> dependency names).
    pub fn handler_dependency_graph(&self) -> &HashMap<String, Vec<String>> {
        &self.handler_dependency_graph
    }

    /// Validate handler dependencies and compute topological ordering.
    ///
    /// Panics if:
    /// - A handler declares a dependency on a name that isn't registered
    /// - The dependency graph contains a cycle
    pub fn validate_and_sort_handler_dependencies(&mut self) {
        // Collect all registered handler names (use name(), not handler_key())
        let registered_names: HashSet<String> = self
            .all_handlers
            .iter()
            .map(|h| h.name().to_string())
            .collect();

        // Validate all dependency references exist
        let mut missing: Vec<(String, Vec<String>)> = Vec::new();
        for (handler_name, deps) in &self.handler_dependency_graph {
            let unresolved: Vec<String> = deps
                .iter()
                .filter(|dep| !registered_names.contains(*dep))
                .cloned()
                .collect();
            if !unresolved.is_empty() {
                missing.push((handler_name.clone(), unresolved));
            }
        }

        if !missing.is_empty() {
            let mut msg = String::from(
                "missing handler dependency: one or more handlers declare handler_dependencies \
                 that do not match any registered handler name:\n",
            );
            for (handler_name, unresolved) in &missing {
                msg.push_str(&format!("\n  Handler '{}':\n", handler_name));
                for dep in unresolved {
                    msg.push_str(&format!("    - \"{}\"\n", dep));
                }
            }
            msg.push_str("\nRegistered handler names:\n");
            let mut sorted_names: Vec<_> = registered_names.iter().collect();
            sorted_names.sort();
            for name in &sorted_names {
                msg.push_str(&format!("    - \"{}\"\n", name));
            }
            panic!("{}", msg);
        }

        // Kahn's algorithm for topological sort
        // Build adjacency: dependency -> set of dependents
        let mut in_degree: HashMap<String, usize> = HashMap::new();
        let mut dependents: HashMap<String, Vec<String>> = HashMap::new();

        // Initialize all handlers with 0 in-degree
        for name in &registered_names {
            in_degree.insert(name.clone(), 0);
        }

        // Build edges from the dependency graph
        for (handler_name, deps) in &self.handler_dependency_graph {
            *in_degree.get_mut(handler_name).unwrap() += deps.len();
            for dep in deps {
                dependents
                    .entry(dep.clone())
                    .or_default()
                    .push(handler_name.clone());
            }
        }

        // Seed queue with zero-in-degree handlers, sorted alphabetically for determinism
        let mut queue: VecDeque<String> = {
            let mut zeros: Vec<String> = in_degree
                .iter()
                .filter(|(_, &deg)| deg == 0)
                .map(|(name, _)| name.clone())
                .collect();
            zeros.sort();
            zeros.into_iter().collect()
        };

        let mut order: Vec<String> = Vec::with_capacity(registered_names.len());

        while let Some(name) = queue.pop_front() {
            order.push(name.clone());
            if let Some(deps) = dependents.get(&name) {
                // Collect newly freed handlers, sort for determinism
                let mut newly_free: Vec<String> = Vec::new();
                for dependent in deps {
                    let deg = in_degree.get_mut(dependent).unwrap();
                    *deg -= 1;
                    if *deg == 0 {
                        newly_free.push(dependent.clone());
                    }
                }
                newly_free.sort();
                for freed in newly_free {
                    queue.push_back(freed);
                }
            }
        }

        if order.len() != registered_names.len() {
            // Cycle detected — find and report it
            let cycle = trace_cycle(&self.handler_dependency_graph, &in_degree);
            panic!(
                "handler dependency cycle detected: {}\n\
                 Handler dependencies must form a directed acyclic graph (DAG).",
                cycle
            );
        }

        self.handler_topological_order = order;
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

/// Trace a cycle in the dependency graph for error reporting.
/// Returns a string like "A -> B -> C -> A".
fn trace_cycle(
    graph: &HashMap<String, Vec<String>>,
    in_degree: &HashMap<String, usize>,
) -> String {
    // Start from any node still with non-zero in-degree (part of a cycle)
    let start = in_degree
        .iter()
        .find(|(_, &deg)| deg > 0)
        .map(|(name, _)| name.clone())
        .unwrap_or_default();

    let mut path = vec![start.clone()];
    let mut current = start.clone();

    // Follow dependency edges to trace the cycle
    for _ in 0..in_degree.len() {
        if let Some(deps) = graph.get(&current) {
            // Follow the first dependency that's part of the cycle (non-zero in-degree)
            if let Some(next) = deps
                .iter()
                .find(|d| in_degree.get(*d).copied().unwrap_or(0) > 0)
            {
                if *next == start {
                    path.push(next.clone());
                    break;
                }
                path.push(next.clone());
                current = next.clone();
            } else {
                break;
            }
        } else {
            break;
        }
    }

    path.join(" -> ")
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
pub fn build_registry() -> TransformationRegistry {
    let mut registry = TransformationRegistry::new();

    // Register event handlers
    super::event::register_handlers(&mut registry);

    // Register eth_call handlers
    super::eth_call::register_handlers(&mut registry);

    registry.validate_and_sort_handler_dependencies();

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
