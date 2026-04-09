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
    /// When set, only handlers whose trigger sources are all present in this
    /// set will be registered. Used to filter handlers per-chain.
    available_sources: Option<HashSet<String>>,
    /// Maps handler name() to its declared handler dependency names
    handler_dependency_graph: HashMap<String, Vec<String>>,
    /// Topological ordering of handler names (computed after all handlers registered)
    handler_topological_order: Vec<String>,
    /// Set of all handler names that appear as a dependency of another handler
    dependency_handler_names: HashSet<String>,
    /// Maps handler_key() to handler name() for reverse lookup
    handler_key_to_name: HashMap<String, String>,
    /// Maps handler name() to handler_key() for reverse lookup
    handler_name_to_key: HashMap<String, String>,
    /// Set of handler names registered as event handlers (for dep validation)
    event_handler_names: HashSet<String>,
    /// Handler keys whose handler declares more than one trigger.
    /// Multi-trigger handlers must not have their success persisted until
    /// all batches for the block have been dispatched.
    multi_trigger_handler_keys: HashSet<String>,
}

impl TransformationRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            event_handlers: HashMap::new(),
            call_handlers: HashMap::new(),
            all_handlers: Vec::new(),
            available_sources: None,
            handler_dependency_graph: HashMap::new(),
            handler_topological_order: Vec::new(),
            dependency_handler_names: HashSet::new(),
            handler_key_to_name: HashMap::new(),
            handler_name_to_key: HashMap::new(),
            event_handler_names: HashSet::new(),
            multi_trigger_handler_keys: HashSet::new(),
        }
    }

    /// Create a registry that filters handlers by available sources.
    ///
    /// Only handlers whose trigger sources are ALL present in the set will
    /// be registered. Handlers with missing sources are silently skipped.
    pub fn with_source_filter(sources: HashSet<String>) -> Self {
        Self {
            event_handlers: HashMap::new(),
            call_handlers: HashMap::new(),
            all_handlers: Vec::new(),
            available_sources: Some(sources),
            handler_dependency_graph: HashMap::new(),
            handler_topological_order: Vec::new(),
            dependency_handler_names: HashSet::new(),
            handler_key_to_name: HashMap::new(),
            handler_name_to_key: HashMap::new(),
            event_handler_names: HashSet::new(),
            multi_trigger_handler_keys: HashSet::new(),
        }
    }

    /// Register an event handler.
    ///
    /// If a source filter is set, the handler is skipped when any of its
    /// trigger sources are missing from the filter set.
    pub fn register_event_handler<H: EventHandler + 'static>(&mut self, handler: H) {
        let handler = Arc::new(handler);
        let name = handler.name().to_string();

        if let Some(existing_key) = self.handler_name_to_key.get(&name) {
            panic!(
                "duplicate handler name '{}': already registered with key '{}', \
                 cannot register again with key '{}'",
                name,
                existing_key,
                handler.handler_key()
            );
        }

        let triggers = handler.triggers();

        if let Some(ref sources) = self.available_sources {
            if !triggers.iter().all(|t| sources.contains(&t.source)) {
                tracing::debug!(
                    "Skipping event handler {} — trigger sources not available for this chain",
                    handler.name(),
                );
                return;
            }
        }

        if triggers.len() > 1 {
            self.multi_trigger_handler_keys
                .insert(handler.handler_key());
        }

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

        self.handler_key_to_name
            .insert(handler.handler_key(), handler.name().to_string());
        self.handler_name_to_key
            .insert(handler.name().to_string(), handler.handler_key());
        self.event_handler_names.insert(handler.name().to_string());

        self.all_handlers.push(handler);
    }

    /// Register an eth_call handler.
    ///
    /// If a source filter is set, the handler is skipped when any of its
    /// trigger sources are missing from the filter set.
    #[allow(dead_code)]
    pub fn register_call_handler<H: EthCallHandler + 'static>(&mut self, handler: H) {
        let handler = Arc::new(handler);
        let name = handler.name().to_string();

        if let Some(existing_key) = self.handler_name_to_key.get(&name) {
            panic!(
                "duplicate handler name '{}': already registered with key '{}', \
                 cannot register again with key '{}'",
                name,
                existing_key,
                handler.handler_key()
            );
        }

        let triggers = handler.triggers();

        if let Some(ref sources) = self.available_sources {
            if !triggers.iter().all(|t| sources.contains(&t.source)) {
                tracing::debug!(
                    "Skipping call handler {} — trigger sources not available for this chain",
                    handler.name(),
                );
                return;
            }
        }

        if triggers.len() > 1 {
            self.multi_trigger_handler_keys
                .insert(handler.handler_key());
        }

        for trigger in triggers {
            let key = (trigger.source.clone(), trigger.function_name.clone());
            self.call_handlers
                .entry(key)
                .or_default()
                .push(handler.clone());
        }

        self.handler_key_to_name
            .insert(handler.handler_key(), name.clone());
        self.handler_name_to_key.insert(name, handler.handler_key());

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

        // Validate all dependencies reference event handlers (not call handlers)
        let mut non_event_deps: Vec<(String, Vec<String>)> = Vec::new();
        for (handler_name, deps) in &self.handler_dependency_graph {
            let bad: Vec<String> = deps
                .iter()
                .filter(|dep| !self.event_handler_names.contains(*dep))
                .cloned()
                .collect();
            if !bad.is_empty() {
                non_event_deps.push((handler_name.clone(), bad));
            }
        }

        if !non_event_deps.is_empty() {
            let mut msg = String::from(
                "invalid handler dependency: handler_dependencies can only reference event \
                 handler names, not call handler names:\n",
            );
            for (handler_name, bad) in &non_event_deps {
                msg.push_str(&format!("\n  Handler '{}':\n", handler_name));
                for dep in bad {
                    msg.push_str(&format!("    - \"{}\" (not an event handler)\n", dep));
                }
            }
            msg.push_str("\nRegistered event handler names:\n");
            let mut sorted: Vec<_> = self.event_handler_names.iter().collect();
            sorted.sort();
            for name in &sorted {
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
        self.dependency_handler_names = self
            .handler_dependency_graph
            .values()
            .flatten()
            .cloned()
            .collect();
    }

    /// Get the set of all handler names that are declared as a dependency by another handler.
    pub fn dependency_handler_names(&self) -> &HashSet<String> {
        &self.dependency_handler_names
    }

    /// Look up a handler's name() from its handler_key().
    pub fn handler_name_for_key(&self, handler_key: &str) -> Option<&str> {
        self.handler_key_to_name
            .get(handler_key)
            .map(|s| s.as_str())
    }

    /// Look up a handler's handler_key() from its name().
    pub fn handler_key_for_name(&self, name: &str) -> Option<&str> {
        self.handler_name_to_key.get(name).map(|s| s.as_str())
    }

    /// Whether a handler was registered with more than one trigger.
    pub fn is_multi_trigger(&self, handler_key: &str) -> bool {
        self.multi_trigger_handler_keys.contains(handler_key)
    }

    /// Get handler keys for all dependency handler names.
    ///
    /// Panics if any dependency name cannot be resolved to a handler key,
    /// which would indicate a registry inconsistency.
    pub fn dependency_handler_keys(&self) -> HashSet<String> {
        self.dependency_handler_names
            .iter()
            .map(|name| {
                self.handler_name_to_key
                    .get(name)
                    .unwrap_or_else(|| {
                        panic!(
                            "dependency handler name '{}' has no registered handler_key",
                            name
                        )
                    })
                    .clone()
            })
            .collect()
    }

    /// Return the transitive set of handler names that depend on `name`.
    ///
    /// Given handler A with dependent B and B with dependent C,
    /// `transitive_dependents_of("A")` returns {"B", "C"}.
    pub fn transitive_dependents_of(&self, name: &str) -> HashSet<String> {
        // Build reverse adjacency: dep -> set of handlers that list it as a dependency
        let mut reverse: HashMap<&str, Vec<&str>> = HashMap::new();
        for (handler, deps) in &self.handler_dependency_graph {
            for dep in deps {
                reverse
                    .entry(dep.as_str())
                    .or_default()
                    .push(handler.as_str());
            }
        }

        let mut result = HashSet::new();
        let mut queue = VecDeque::new();
        if let Some(direct) = reverse.get(name) {
            for d in direct {
                queue.push_back(*d);
            }
        }
        while let Some(current) = queue.pop_front() {
            if result.insert(current.to_string()) {
                if let Some(further) = reverse.get(current) {
                    for d in further {
                        queue.push_back(*d);
                    }
                }
            }
        }
        result
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
fn trace_cycle(graph: &HashMap<String, Vec<String>>, in_degree: &HashMap<String, usize>) -> String {
    // Start from the alphabetically first node with non-zero in-degree (deterministic)
    let start = {
        let mut candidates: Vec<_> = in_degree
            .iter()
            .filter(|(_, &deg)| deg > 0)
            .map(|(name, _)| name.clone())
            .collect();
        candidates.sort();
        candidates.into_iter().next().unwrap_or_default()
    };

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

/// Build the transformation registry with all handlers (unfiltered).
///
/// This is where handlers are registered at compile-time.
/// Add new handler registrations here as they are implemented.
pub fn build_registry(chain_id: u64) -> TransformationRegistry {
    let mut registry = TransformationRegistry::new();

    // Register event handlers
    super::event::register_handlers(&mut registry, chain_id);

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

/// Build a transformation registry filtered to handlers relevant for a specific chain.
///
/// Only handlers whose trigger sources all exist in the chain's contracts or
/// factory collections are registered. This prevents handlers designed for one
/// chain from being initialized, migrated, or validated on another.
pub fn build_registry_for_chain(
    chain_id: u64,
    contracts: &Contracts,
    factory_collections: &FactoryCollections,
) -> TransformationRegistry {
    let mut available: HashSet<String> = contracts.keys().cloned().collect();
    available.extend(factory_collections.keys().cloned());

    // Include factory collection names from contract configs so that handlers
    // triggering on collection sources (e.g., "DERC20") pass the source filter.
    for contract in contracts.values() {
        if let Some(factories) = &contract.factories {
            for factory in factories {
                available.insert(factory.collection.clone());
            }
        }
    }

    let mut registry = TransformationRegistry::with_source_filter(available);

    // Register event handlers (filtered by available sources)
    super::event::register_handlers_for_chain(&mut registry, chain_id, contracts);

    // Register eth_call handlers (filtered by available sources)
    super::eth_call::register_handlers(&mut registry);

    registry.validate_and_sort_handler_dependencies();

    tracing::info!(
        "Built chain-filtered transformation registry with {} handlers ({} event triggers, {} call triggers)",
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
        handler_deps: Vec<&'static str>,
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

        fn handler_dependencies(&self) -> Vec<&'static str> {
            self.handler_deps.clone()
        }
    }

    fn empty_contracts() -> Contracts {
        Contracts::new()
    }

    fn empty_factory_collections() -> FactoryCollections {
        FactoryCollections::new()
    }

    fn mock_handler(name: &'static str, handler_deps: Vec<&'static str>) -> MockEventHandler {
        MockEventHandler {
            name,
            triggers: vec![EventTrigger::new("Test", format!("{}()", name))],
            call_deps: vec![],
            handler_deps,
        }
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
            handler_deps: vec![],
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
            handler_deps: vec![],
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
            handler_deps: vec![],
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
            handler_deps: vec![],
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
            handler_deps: vec![],
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

    #[test]
    fn handler_deps_empty_registry_passes() {
        let mut registry = TransformationRegistry::new();
        // Should not panic — no handlers, no deps
        registry.validate_and_sort_handler_dependencies();
        assert!(registry.handler_topological_order().is_empty());
    }

    #[test]
    fn handler_deps_no_deps_passes() {
        let mut registry = TransformationRegistry::new();
        registry.register_event_handler(mock_handler("A", vec![]));
        registry.register_event_handler(mock_handler("B", vec![]));
        registry.validate_and_sort_handler_dependencies();
        assert_eq!(registry.handler_topological_order().len(), 2);
    }

    #[test]
    fn handler_deps_linear_chain() {
        let mut registry = TransformationRegistry::new();
        // C depends on B, B depends on A
        registry.register_event_handler(mock_handler("C", vec!["B"]));
        registry.register_event_handler(mock_handler("A", vec![]));
        registry.register_event_handler(mock_handler("B", vec!["A"]));
        registry.validate_and_sort_handler_dependencies();
        let order = registry.handler_topological_order();
        let pos_a = order.iter().position(|n| n == "A").unwrap();
        let pos_b = order.iter().position(|n| n == "B").unwrap();
        let pos_c = order.iter().position(|n| n == "C").unwrap();
        assert!(pos_a < pos_b, "A must come before B");
        assert!(pos_b < pos_c, "B must come before C");
    }

    #[test]
    fn handler_deps_diamond() {
        let mut registry = TransformationRegistry::new();
        // Diamond: A -> B, A -> C, B -> D, C -> D
        registry.register_event_handler(mock_handler("D", vec!["B", "C"]));
        registry.register_event_handler(mock_handler("B", vec!["A"]));
        registry.register_event_handler(mock_handler("C", vec!["A"]));
        registry.register_event_handler(mock_handler("A", vec![]));
        registry.validate_and_sort_handler_dependencies();
        let order = registry.handler_topological_order();
        let pos_a = order.iter().position(|n| n == "A").unwrap();
        let pos_b = order.iter().position(|n| n == "B").unwrap();
        let pos_c = order.iter().position(|n| n == "C").unwrap();
        let pos_d = order.iter().position(|n| n == "D").unwrap();
        assert!(pos_a < pos_b, "A must come before B");
        assert!(pos_a < pos_c, "A must come before C");
        assert!(pos_b < pos_d, "B must come before D");
        assert!(pos_c < pos_d, "C must come before D");
    }

    #[test]
    #[should_panic(expected = "cycle")]
    fn handler_deps_two_node_cycle_panics() {
        let mut registry = TransformationRegistry::new();
        registry.register_event_handler(mock_handler("A", vec!["B"]));
        registry.register_event_handler(mock_handler("B", vec!["A"]));
        registry.validate_and_sort_handler_dependencies();
    }

    #[test]
    #[should_panic(expected = "cycle")]
    fn handler_deps_three_node_cycle_panics() {
        let mut registry = TransformationRegistry::new();
        registry.register_event_handler(mock_handler("A", vec!["C"]));
        registry.register_event_handler(mock_handler("B", vec!["A"]));
        registry.register_event_handler(mock_handler("C", vec!["B"]));
        registry.validate_and_sort_handler_dependencies();
    }

    #[test]
    #[should_panic(expected = "cycle")]
    fn handler_deps_self_cycle_panics() {
        let mut registry = TransformationRegistry::new();
        registry.register_event_handler(mock_handler("A", vec!["A"]));
        registry.validate_and_sort_handler_dependencies();
    }

    #[test]
    #[should_panic(expected = "missing handler dependency")]
    fn handler_deps_missing_dep_panics() {
        let mut registry = TransformationRegistry::new();
        registry.register_event_handler(mock_handler("A", vec!["NonExistent"]));
        registry.validate_and_sort_handler_dependencies();
    }

    #[test]
    fn handler_deps_deterministic_ordering() {
        // Run twice to verify determinism
        for _ in 0..2 {
            let mut registry = TransformationRegistry::new();
            // All independent — should sort alphabetically
            registry.register_event_handler(mock_handler("C", vec![]));
            registry.register_event_handler(mock_handler("A", vec![]));
            registry.register_event_handler(mock_handler("B", vec![]));
            registry.validate_and_sort_handler_dependencies();
            let order = registry.handler_topological_order();
            assert_eq!(order, &["A", "B", "C"]);
        }
    }

    // ─── Call handler dep validation tests ──────────────────────────────

    use crate::transformations::traits::{EthCallHandler, EthCallTrigger};

    struct MockCallHandler {
        name: &'static str,
    }

    #[async_trait]
    impl TransformationHandler for MockCallHandler {
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

    impl EthCallHandler for MockCallHandler {
        fn triggers(&self) -> Vec<EthCallTrigger> {
            vec![EthCallTrigger::new("Test", self.name)]
        }
    }

    #[test]
    #[should_panic(expected = "not an event handler")]
    fn handler_deps_referencing_call_handler_panics() {
        let mut registry = TransformationRegistry::new();
        registry.register_event_handler(mock_handler("A", vec![]));
        registry.register_call_handler(MockCallHandler { name: "B" });
        registry.register_event_handler(mock_handler("C", vec!["B"]));
        registry.validate_and_sort_handler_dependencies();
    }

    #[test]
    fn handler_deps_with_call_handlers_and_valid_event_deps_passes() {
        let mut registry = TransformationRegistry::new();
        registry.register_event_handler(mock_handler("A", vec![]));
        registry.register_call_handler(MockCallHandler { name: "B" });
        registry.register_event_handler(mock_handler("C", vec!["A"]));
        registry.validate_and_sort_handler_dependencies();
        let order = registry.handler_topological_order();
        let pos_a = order.iter().position(|n| n == "A").unwrap();
        let pos_c = order.iter().position(|n| n == "C").unwrap();
        assert!(pos_a < pos_c, "A must come before C");
    }

    #[test]
    fn transitive_dependents_no_deps() {
        let mut registry = TransformationRegistry::new();
        registry.register_event_handler(mock_handler("A", vec![]));
        registry.register_event_handler(mock_handler("B", vec![]));
        registry.validate_and_sort_handler_dependencies();
        assert!(registry.transitive_dependents_of("A").is_empty());
    }

    #[test]
    fn transitive_dependents_linear_chain() {
        let mut registry = TransformationRegistry::new();
        registry.register_event_handler(mock_handler("A", vec![]));
        registry.register_event_handler(mock_handler("B", vec!["A"]));
        registry.register_event_handler(mock_handler("C", vec!["B"]));
        registry.validate_and_sort_handler_dependencies();
        let deps_of_a = registry.transitive_dependents_of("A");
        assert_eq!(deps_of_a.len(), 2);
        assert!(deps_of_a.contains("B"));
        assert!(deps_of_a.contains("C"));
        let deps_of_b = registry.transitive_dependents_of("B");
        assert_eq!(deps_of_b.len(), 1);
        assert!(deps_of_b.contains("C"));
        assert!(registry.transitive_dependents_of("C").is_empty());
    }

    #[test]
    fn transitive_dependents_diamond() {
        let mut registry = TransformationRegistry::new();
        registry.register_event_handler(mock_handler("A", vec![]));
        registry.register_event_handler(mock_handler("B", vec!["A"]));
        registry.register_event_handler(mock_handler("C", vec!["A"]));
        registry.register_event_handler(mock_handler("D", vec!["B", "C"]));
        registry.validate_and_sort_handler_dependencies();
        let deps_of_a = registry.transitive_dependents_of("A");
        assert_eq!(deps_of_a.len(), 3);
        assert!(deps_of_a.contains("B"));
        assert!(deps_of_a.contains("C"));
        assert!(deps_of_a.contains("D"));
    }

    #[test]
    fn handler_key_for_name_lookup() {
        let mut registry = TransformationRegistry::new();
        registry.register_event_handler(mock_handler("A", vec![]));
        assert!(registry.handler_key_for_name("A").is_some());
        assert!(registry.handler_key_for_name("nonexistent").is_none());
    }

    #[test]
    fn single_trigger_handler_is_not_multi_trigger() {
        let mut registry = TransformationRegistry::new();
        registry.register_event_handler(mock_handler("A", vec![]));
        assert!(!registry.is_multi_trigger(&"A_v1".to_string()));
    }

    #[test]
    fn multi_trigger_event_handler_detected() {
        let mut registry = TransformationRegistry::new();
        registry.register_event_handler(MockEventHandler {
            name: "multi",
            triggers: vec![
                EventTrigger::new("Source1", "Event1()"),
                EventTrigger::new("Source2", "Event2()"),
            ],
            call_deps: vec![],
            handler_deps: vec![],
        });
        assert!(registry.is_multi_trigger("multi_v1"));
    }

    #[test]
    fn multi_trigger_call_handler_detected() {
        use crate::transformations::traits::{EthCallHandler, EthCallTrigger};

        struct MockCallHandler;

        #[async_trait]
        impl TransformationHandler for MockCallHandler {
            fn name(&self) -> &'static str {
                "multi_call"
            }
            async fn handle(
                &self,
                _ctx: &TransformationContext,
            ) -> Result<Vec<DbOperation>, TransformationError> {
                Ok(vec![])
            }
        }

        impl EthCallHandler for MockCallHandler {
            fn triggers(&self) -> Vec<EthCallTrigger> {
                vec![
                    EthCallTrigger::new("Pool1", "slot0"),
                    EthCallTrigger::new("Pool2", "slot0"),
                ]
            }
        }

        let mut registry = TransformationRegistry::new();
        registry.register_call_handler(MockCallHandler);
        assert!(registry.is_multi_trigger("multi_call_v1"));
    }

    #[test]
    fn dependency_handler_keys_resolves_all_names() {
        let mut registry = TransformationRegistry::new();
        registry.register_event_handler(mock_handler("A", vec![]));
        registry.register_event_handler(mock_handler("B", vec!["A"]));
        registry.validate_and_sort_handler_dependencies();
        let dep_keys = registry.dependency_handler_keys();
        assert_eq!(dep_keys.len(), 1);
        assert!(dep_keys.contains("A_v1"));
    }

    #[test]
    #[should_panic(expected = "duplicate handler name 'A'")]
    fn duplicate_event_handler_name_panics() {
        let mut registry = TransformationRegistry::new();
        registry.register_event_handler(mock_handler("A", vec![]));
        registry.register_event_handler(mock_handler("A", vec![]));
    }

    #[test]
    #[should_panic(expected = "duplicate handler name 'B'")]
    fn duplicate_call_handler_name_panics() {
        let mut registry = TransformationRegistry::new();
        registry.register_call_handler(MockCallHandler { name: "B" });
        registry.register_call_handler(MockCallHandler { name: "B" });
    }

    #[test]
    #[should_panic(expected = "duplicate handler name 'X'")]
    fn duplicate_name_across_event_and_call_panics() {
        let mut registry = TransformationRegistry::new();
        registry.register_event_handler(mock_handler("X", vec![]));
        registry.register_call_handler(MockCallHandler { name: "X" });
    }

    #[test]
    fn build_registry_for_chain_skips_migration_pool_handlers_without_migrator() {
        use crate::types::config::contract::{AddressOrAddresses, ContractConfig};
        use alloy_primitives::Address;

        let mut contracts = Contracts::new();
        contracts.insert(
            "UniswapV4PoolManager".to_string(),
            ContractConfig {
                address: AddressOrAddresses::Single(Address::ZERO),
                start_block: None,
                calls: None,
                factories: None,
                events: None,
            },
        );

        let registry = build_registry_for_chain(1, &contracts, &empty_factory_collections());

        assert!(registry
            .handler_key_for_name("MigrationPoolCreateHandler")
            .is_none());
        assert!(registry
            .handler_key_for_name("MigrationPoolSwapMetricsHandler")
            .is_none());
        assert!(registry
            .handler_key_for_name("MigrationPoolLiquidityMetricsHandler")
            .is_none());
    }

    #[test]
    fn build_registry_declares_all_migration_pool_create_dependencies() {
        let registry = build_registry(1);
        let deps = registry
            .handler_dependency_graph()
            .get("MigrationPoolCreateHandler")
            .expect("MigrationPoolCreateHandler should be registered");

        assert_eq!(
            deps,
            &vec![
                "V4CreateHandler".to_string(),
                "V4MulticurveCreateHandler".to_string(),
                "V4ScheduledMulticurveCreateHandler".to_string(),
                "V4DecayMulticurveCreateHandler".to_string(),
                "DopplerHookCreateHandler".to_string(),
            ]
        );
    }
}
