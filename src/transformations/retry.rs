//! Live retry processing for blocks that need re-transformation.
//!
//! Handles reading stored live data (decoded logs, calls) from bincode storage,
//! converting them to the unified decoded types, and re-executing handlers
//! for blocks that had missing or failed transformations.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;

use super::context::{DecodedCall, DecodedEvent, TransactionAddresses, TransformationContext};
use super::error::TransformationError;
use super::executor::{execute_with_snapshot_capture, inject_source_version};
use super::historical::HistoricalDataReader;
use super::live_state::LiveProcessingState;
use super::registry::{extract_event_name, TransformationRegistry};
use crate::decoding::eth_calls::{
    build_decode_configs, build_result_map, CallDecodeConfig, EventCallDecodeConfig,
};
use crate::decoding::event_parsing::ParsedEvent;
use crate::decoding::logs::build_event_matchers;
use crate::live::{LiveProgressTracker, LiveStorage, TransformRetryRequest};
use crate::rpc::UnifiedRpcClient;
use crate::types::config::contract::{Contracts, FactoryCollections};
use crate::types::config::eth_call::EvmType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LiveRetryCallArtifactKind {
    Regular,
    EventTriggered { base_name: String },
}

pub(crate) fn resolve_retry_missing_handlers(
    request_missing: Option<HashSet<String>>,
    tracker_missing: Option<HashSet<String>>,
    all_handlers: HashSet<String>,
) -> HashSet<String> {
    tracker_missing.unwrap_or_else(|| request_missing.unwrap_or(all_handlers))
}

pub(crate) fn missing_retry_call_dependencies(
    required_calls: &HashSet<(String, String)>,
    calls: &[DecodedCall],
) -> HashSet<(String, String)> {
    let available_calls: HashSet<(String, String)> = calls
        .iter()
        .map(|call| (call.source_name.clone(), call.function_name.clone()))
        .collect();

    required_calls
        .difference(&available_calls)
        .cloned()
        .collect()
}

pub(crate) fn classify_live_retry_call_artifact(
    source_name: &str,
    function_name: &str,
    regular_keys: &HashSet<(String, String)>,
    event_keys: &HashSet<(String, String)>,
) -> Result<LiveRetryCallArtifactKind, TransformationError> {
    let exact_key = (source_name.to_string(), function_name.to_string());
    if regular_keys.contains(&exact_key) {
        return Ok(LiveRetryCallArtifactKind::Regular);
    }

    if let Some(base_name) = function_name.strip_suffix("_event") {
        let event_key = (source_name.to_string(), base_name.to_string());
        if event_keys.contains(&event_key) {
            return Ok(LiveRetryCallArtifactKind::EventTriggered {
                base_name: base_name.to_string(),
            });
        }
    }

    Err(TransformationError::MissingData(format!(
        "missing call schema for live retry {}/{}",
        source_name, function_name
    )))
}

/// Processes transform retries for blocks that need re-transformation.
pub(crate) struct RetryProcessor {
    pub registry: Arc<TransformationRegistry>,
    pub db_pool: Arc<DbPool>,
    pub rpc_client: Arc<UnifiedRpcClient>,
    pub historical_reader: Arc<HistoricalDataReader>,
    pub contracts: Arc<Contracts>,
    pub factory_collections: Arc<FactoryCollections>,
    pub chain_name: String,
    pub chain_id: u64,
    pub handler_concurrency: usize,
    pub progress_tracker: Option<Arc<Mutex<LiveProgressTracker>>>,
}

use crate::db::DbPool;

impl RetryProcessor {
    pub async fn process_transform_retry(
        &self,
        request: TransformRetryRequest,
        live_state: &Mutex<LiveProcessingState>,
        record_and_finalize: &dyn RecordAndFinalize,
    ) -> Result<(), TransformationError> {
        let block_number = request.block_number;
        let range_key = (block_number, block_number + 1);

        tracing::info!(
            "Processing direct transform retry for block {}",
            block_number
        );

        {
            let mut state = live_state.lock().await;
            state.cleanup_for_retry(range_key);
        }

        let tracker_missing = if let Some(ref tracker) = self.progress_tracker {
            Some(tracker.lock().await.get_pending_handlers(block_number))
        } else {
            None
        };
        let all_handlers: HashSet<String> = self
            .registry
            .all_handlers()
            .iter()
            .map(|handler| handler.handler_key())
            .collect();
        let missing_handlers =
            resolve_retry_missing_handlers(request.missing_handlers, tracker_missing, all_handlers);

        if missing_handlers.is_empty() {
            return record_and_finalize
                .finalize_range(block_number, block_number + 1)
                .await;
        }

        let (events, calls) = self.read_live_retry_data(block_number).await?;
        let events = filter_events_by_start_block(&self.contracts, events);
        let calls = filter_calls_by_start_block(&self.contracts, calls);

        let blocked_handlers = self
            .execute_live_retry_handlers(block_number, events, calls, &missing_handlers)
            .await?;

        if !blocked_handlers.is_empty() {
            tracing::warn!(
                "Live retry for block {} is still waiting on call dependencies for handlers {:?}",
                block_number,
                blocked_handlers
            );
            return Ok(());
        }

        record_and_finalize
            .finalize_range(block_number, block_number + 1)
            .await
    }

    async fn read_live_retry_data(
        &self,
        block_number: u64,
    ) -> Result<(Vec<DecodedEvent>, Vec<DecodedCall>), TransformationError> {
        let storage = LiveStorage::new(&self.chain_name);
        let mut events = Vec::new();
        let mut calls = Vec::new();

        let (regular_matchers, factory_matchers) =
            build_event_matchers(&self.contracts, &self.factory_collections).map_err(|e| {
                TransformationError::DecodeError(format!(
                    "failed to build live retry event matchers: {}",
                    e
                ))
            })?;

        let mut event_schemas: HashMap<(String, String), ParsedEvent> = HashMap::new();
        for matcher in regular_matchers {
            event_schemas.insert(
                (matcher.name.clone(), matcher.event_name.clone()),
                matcher.event,
            );
        }
        for matchers in factory_matchers.values() {
            for matcher in matchers {
                event_schemas.insert(
                    (matcher.name.clone(), matcher.event_name.clone()),
                    matcher.event.clone(),
                );
            }
        }

        for (source_name, event_name) in storage.list_decoded_log_types(block_number)? {
            let parsed_event = event_schemas
                .get(&(source_name.clone(), event_name.clone()))
                .ok_or_else(|| {
                    TransformationError::MissingData(format!(
                        "missing event schema for live retry {}/{}",
                        source_name, event_name
                    ))
                })?;

            for log in storage.read_decoded_logs(block_number, &source_name, &event_name)? {
                events.push(live_log_to_decoded_event(
                    &log,
                    parsed_event,
                    &source_name,
                    &event_name,
                ));
            }
        }

        let (regular_configs, once_configs, event_configs) =
            build_decode_configs(&self.contracts);
        let regular_map: HashMap<(String, String), CallDecodeConfig> = regular_configs
            .into_iter()
            .map(|config| {
                (
                    (config.contract_name.clone(), config.function_name.clone()),
                    config,
                )
            })
            .collect();
        let once_map: HashMap<(String, String), CallDecodeConfig> = once_configs
            .into_iter()
            .map(|config| {
                (
                    (config.contract_name.clone(), config.function_name.clone()),
                    config,
                )
            })
            .collect();
        let event_map: HashMap<(String, String), EventCallDecodeConfig> = event_configs
            .into_iter()
            .map(|config| {
                (
                    (config.contract_name.clone(), config.function_name.clone()),
                    config,
                )
            })
            .collect();
        let regular_keys: HashSet<(String, String)> = regular_map.keys().cloned().collect();
        let event_keys: HashSet<(String, String)> = event_map.keys().cloned().collect();

        for (source_name, function_name) in storage.list_decoded_call_types(block_number)? {
            match classify_live_retry_call_artifact(
                &source_name,
                &function_name,
                &regular_keys,
                &event_keys,
            )? {
                LiveRetryCallArtifactKind::Regular => {
                    let config = regular_map
                        .get(&(source_name.clone(), function_name.clone()))
                        .ok_or_else(|| {
                            TransformationError::MissingData(format!(
                                "missing regular call schema for live retry {}/{}",
                                source_name, function_name
                            ))
                        })?;

                    for call in
                        storage.read_decoded_calls(block_number, &source_name, &function_name)?
                    {
                        calls.push(live_call_to_decoded_call(
                            &call,
                            &source_name,
                            &function_name,
                            &config.output_type,
                        ));
                    }
                }
                LiveRetryCallArtifactKind::EventTriggered { base_name } => {
                    let config = event_map
                        .get(&(source_name.clone(), base_name.clone()))
                        .ok_or_else(|| {
                            TransformationError::MissingData(format!(
                                "missing event call schema for live retry {}/{}",
                                source_name, base_name
                            ))
                        })?;

                    for call in
                        storage.read_decoded_event_calls(block_number, &source_name, &base_name)?
                    {
                        calls.push(live_event_call_to_decoded_call(
                            &call,
                            &source_name,
                            &base_name,
                            &config.output_type,
                        ));
                    }
                }
            }
        }

        let mut once_sources: HashSet<String> = self.contracts.keys().cloned().collect();
        for contract in self.contracts.values() {
            if let Some(factories) = &contract.factories {
                once_sources.extend(factories.iter().map(|factory| factory.collection.clone()));
            }
        }

        for source_name in once_sources {
            let Ok(once_calls) = storage.read_decoded_once_calls(block_number, &source_name)
            else {
                continue;
            };

            for call in once_calls {
                let mut merged_result = HashMap::new();
                for (function_name, value) in &call.decoded_values {
                    if let Some(config) =
                        once_map.get(&(source_name.clone(), function_name.clone()))
                    {
                        let partial_result =
                            build_result_map(value, &config.output_type, function_name);
                        merged_result.extend(partial_result);
                    }
                }
                if !merged_result.is_empty() {
                    calls.push(DecodedCall {
                        block_number: call.block_number,
                        block_timestamp: call.block_timestamp,
                        contract_address: call.contract_address,
                        source_name: source_name.clone(),
                        function_name: "once".to_string(),
                        trigger_log_index: None,
                        result: merged_result,
                    });
                }
            }
        }

        Ok((events, calls))
    }

    async fn execute_live_retry_handlers(
        &self,
        block_number: u64,
        events: Vec<DecodedEvent>,
        calls: Vec<DecodedCall>,
        missing_handlers: &HashSet<String>,
    ) -> Result<HashSet<String>, TransformationError> {
        let range_start = block_number;
        let range_end = block_number + 1;
        let tx_addresses = Arc::new(read_live_receipt_addresses(
            &self.chain_name,
            block_number,
        )?);
        let semaphore = Arc::new(Semaphore::new(self.handler_concurrency));
        let mut join_set: JoinSet<Result<Option<String>, TransformationError>> = JoinSet::new();
        let mut blocked_handlers = HashSet::new();

        for handler_info in self.registry.unique_event_handlers() {
            let handler = handler_info.handler;
            let handler_key = handler.handler_key();
            if !missing_handlers.contains(&handler_key) {
                continue;
            }

            let triggers: HashSet<(String, String)> = handler_info
                .triggers
                .iter()
                .map(|trigger| {
                    (
                        trigger.source.clone(),
                        extract_event_name(&trigger.event_signature),
                    )
                })
                .collect();
            let handler_events: Vec<DecodedEvent> = events
                .iter()
                .filter(|event| {
                    triggers.contains(&(event.source_name.clone(), event.event_name.clone()))
                })
                .cloned()
                .collect();

            if handler_events.is_empty() {
                continue;
            }

            let call_deps: HashSet<(String, String)> =
                handler.call_dependencies().into_iter().collect();
            let missing_deps = missing_retry_call_dependencies(&call_deps, &calls);
            if !missing_deps.is_empty() {
                tracing::warn!(
                    "Skipping live retry for handler {} on block {}: missing call dependencies {:?}",
                    handler_key,
                    block_number,
                    missing_deps
                );
                blocked_handlers.insert(handler_key);
                continue;
            }
            let handler_calls: Vec<DecodedCall> = calls
                .iter()
                .filter(|call| {
                    call_deps.contains(&(call.source_name.clone(), call.function_name.clone()))
                })
                .cloned()
                .collect();

            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let db_pool = self.db_pool.clone();
            let chain_name = self.chain_name.clone();
            let chain_id = self.chain_id;
            let historical = self.historical_reader.clone();
            let rpc = self.rpc_client.clone();
            let contracts = self.contracts.clone();
            let tx_addresses = tx_addresses.clone();
            let handler_name = handler.name();
            let handler_version = handler.version();

            join_set.spawn(async move {
                let _permit = permit;
                let live_storage = LiveStorage::new(&chain_name);
                let ctx = TransformationContext::new(
                    chain_name,
                    chain_id,
                    range_start,
                    range_end,
                    Arc::new(handler_events),
                    Arc::new(handler_calls),
                    (*tx_addresses).clone(),
                    historical,
                    rpc,
                    contracts,
                );

                match handler.handle(&ctx).await {
                    Ok(ops) => {
                        if !ops.is_empty() {
                            let ops =
                                inject_source_version(ops, handler_name, handler_version);
                            execute_with_snapshot_capture(
                                ops,
                                &db_pool,
                                Some(&live_storage),
                                range_start,
                                handler_name,
                                handler_version,
                            )
                            .await?;
                        }
                        Ok(Some(handler_key))
                    }
                    Err(e) => Err(e),
                }
            });
        }

        for handler_info in self.registry.unique_call_handlers() {
            let handler = handler_info.handler;
            let handler_key = handler.handler_key();
            if !missing_handlers.contains(&handler_key) {
                continue;
            }

            let triggers: HashSet<(String, String)> = handler_info
                .triggers
                .iter()
                .map(|trigger| (trigger.source.clone(), trigger.function_name.clone()))
                .collect();
            let handler_calls: Vec<DecodedCall> = calls
                .iter()
                .filter(|call| {
                    triggers.contains(&(call.source_name.clone(), call.function_name.clone()))
                })
                .cloned()
                .collect();

            if handler_calls.is_empty() {
                continue;
            }

            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let db_pool = self.db_pool.clone();
            let chain_name = self.chain_name.clone();
            let chain_id = self.chain_id;
            let historical = self.historical_reader.clone();
            let rpc = self.rpc_client.clone();
            let contracts = self.contracts.clone();
            let handler_name = handler.name();
            let handler_version = handler.version();

            join_set.spawn(async move {
                let _permit = permit;
                let live_storage = LiveStorage::new(&chain_name);
                let ctx = TransformationContext::new(
                    chain_name,
                    chain_id,
                    range_start,
                    range_end,
                    Arc::new(Vec::new()),
                    Arc::new(handler_calls),
                    HashMap::new(),
                    historical,
                    rpc,
                    contracts,
                );

                match handler.handle(&ctx).await {
                    Ok(ops) => {
                        if !ops.is_empty() {
                            let ops =
                                inject_source_version(ops, handler_name, handler_version);
                            execute_with_snapshot_capture(
                                ops,
                                &db_pool,
                                Some(&live_storage),
                                range_start,
                                handler_name,
                                handler_version,
                            )
                            .await?;
                        }
                        Ok(Some(handler_key))
                    }
                    Err(e) => Err(e),
                }
            });
        }

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(Some(handler_key))) => {
                    if let Err(e) = self
                        .db_pool
                        .execute_transaction(vec![crate::db::DbOperation::Upsert {
                            table: "_handler_progress".to_string(),
                            columns: vec![
                                "chain_id".to_string(),
                                "handler_key".to_string(),
                                "range_start".to_string(),
                                "range_end".to_string(),
                            ],
                            values: vec![
                                crate::db::DbValue::Int64(self.chain_id as i64),
                                crate::db::DbValue::Text(handler_key.clone()),
                                crate::db::DbValue::Int64(range_start as i64),
                                crate::db::DbValue::Int64(range_end as i64),
                            ],
                            conflict_columns: vec![
                                "chain_id".to_string(),
                                "handler_key".to_string(),
                                "range_start".to_string(),
                            ],
                            update_columns: vec!["range_end".to_string()],
                        }])
                        .await
                    {
                        tracing::warn!(
                            "Failed to record completed range for handler {} on block {}: {}",
                            handler_key,
                            block_number,
                            e
                        );
                    }

                    if let Some(ref tracker) = self.progress_tracker {
                        let mut tracker = tracker.lock().await;
                        if let Err(e) = tracker.mark_complete(block_number, &handler_key).await {
                            tracing::warn!(
                                "Failed to mark retry progress for block {} handler {}: {}",
                                block_number,
                                handler_key,
                                e
                            );
                        }
                    }
                }
                Ok(Ok(None)) => {}
                Ok(Err(e)) => {
                    tracing::error!(
                        "Handler failed during live retry for block {}: {}",
                        block_number,
                        e
                    );
                }
                Err(e) => {
                    tracing::error!(
                        "Handler task panicked during live retry for block {}: {}",
                        block_number,
                        e
                    );
                }
            }
        }

        Ok(blocked_handlers)
    }
}

// ─── Trait for finalization callback ────────────────────────────────

/// Trait to allow RetryProcessor to call back into the engine for finalization.
#[async_trait::async_trait]
pub(crate) trait RecordAndFinalize: Send + Sync {
    async fn finalize_range(
        &self,
        range_start: u64,
        range_end: u64,
    ) -> Result<(), TransformationError>;
}

// ─── Live adapter functions ──────────────────────────────────────────

fn live_log_to_decoded_event(
    log: &crate::live::LiveDecodedLog,
    parsed_event: &ParsedEvent,
    source_name: &str,
    event_name: &str,
) -> DecodedEvent {
    let mut params = HashMap::new();
    for (flattened, value) in parsed_event
        .flattened_fields
        .iter()
        .zip(log.decoded_values.iter())
    {
        params.insert(flattened.full_name.clone(), value.clone());
    }

    DecodedEvent {
        block_number: log.block_number,
        block_timestamp: log.block_timestamp,
        transaction_hash: log.transaction_hash,
        log_index: log.log_index,
        contract_address: log.contract_address,
        source_name: source_name.to_string(),
        event_name: event_name.to_string(),
        event_signature: parsed_event.signature.clone(),
        params,
    }
}

fn live_call_to_decoded_call(
    call: &crate::live::LiveDecodedCall,
    source_name: &str,
    function_name: &str,
    output_type: &EvmType,
) -> DecodedCall {
    DecodedCall {
        block_number: call.block_number,
        block_timestamp: call.block_timestamp,
        contract_address: call.contract_address,
        source_name: source_name.to_string(),
        function_name: function_name.to_string(),
        trigger_log_index: None,
        result: build_result_map(&call.decoded_value, output_type, function_name),
    }
}

fn live_event_call_to_decoded_call(
    call: &crate::live::LiveDecodedEventCall,
    source_name: &str,
    function_name: &str,
    output_type: &EvmType,
) -> DecodedCall {
    DecodedCall {
        block_number: call.block_number,
        block_timestamp: call.block_timestamp,
        contract_address: call.target_address,
        source_name: source_name.to_string(),
        function_name: function_name.to_string(),
        trigger_log_index: Some(call.log_index),
        result: build_result_map(&call.decoded_value, output_type, function_name),
    }
}

fn read_live_receipt_addresses(
    chain_name: &str,
    block_number: u64,
) -> Result<HashMap<[u8; 32], TransactionAddresses>, TransformationError> {
    let storage = LiveStorage::new(chain_name);
    let mut tx_addresses = HashMap::new();

    for receipt in storage.read_receipts(block_number)? {
        tx_addresses.insert(
            receipt.transaction_hash,
            TransactionAddresses {
                from_address: receipt.from,
                to_address: receipt.to,
            },
        );
    }

    Ok(tx_addresses)
}

// ─── Start block filtering helpers ───────────────────────────────────

pub(crate) fn filter_events_by_start_block(
    contracts: &Contracts,
    events: Vec<DecodedEvent>,
) -> Vec<DecodedEvent> {
    events
        .into_iter()
        .filter(|e| {
            let start_block = contracts
                .get(&e.source_name)
                .and_then(|c| c.start_block.map(|u| u.to::<u64>()));
            start_block.map_or(true, |sb| e.block_number >= sb)
        })
        .collect()
}

pub(crate) fn filter_calls_by_start_block(
    contracts: &Contracts,
    calls: Vec<DecodedCall>,
) -> Vec<DecodedCall> {
    calls
        .into_iter()
        .filter(|c| {
            let start_block = contracts
                .get(&c.source_name)
                .and_then(|ct| ct.start_block.map(|u| u.to::<u64>()));
            start_block.map_or(true, |sb| c.block_number >= sb)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transformations::context::DecodedValue;

    #[test]
    fn retry_missing_handlers_prefers_current_tracker_state() {
        let requested = Some(HashSet::from([
            "handler_a_v1".to_string(),
            "handler_b_v1".to_string(),
        ]));
        let tracker = Some(HashSet::from(["handler_b_v1".to_string()]));
        let all_handlers = HashSet::from([
            "handler_a_v1".to_string(),
            "handler_b_v1".to_string(),
            "handler_c_v1".to_string(),
        ]);

        let resolved = resolve_retry_missing_handlers(requested, tracker, all_handlers);
        assert_eq!(resolved, HashSet::from(["handler_b_v1".to_string()]));
    }

    #[test]
    fn retry_dependency_check_detects_missing_calls() {
        let required = HashSet::from([
            ("Pool".to_string(), "slot0".to_string()),
            ("Pool".to_string(), "liquidity".to_string()),
        ]);
        let calls = vec![DecodedCall {
            block_number: 100,
            block_timestamp: 1200,
            contract_address: [0; 20],
            source_name: "Pool".to_string(),
            function_name: "slot0".to_string(),
            trigger_log_index: None,
            result: HashMap::from([("result".to_string(), DecodedValue::Uint64(1))]),
        }];

        let missing = missing_retry_call_dependencies(&required, &calls);
        assert_eq!(
            missing,
            HashSet::from([("Pool".to_string(), "liquidity".to_string())])
        );
    }

    #[test]
    fn live_retry_call_artifact_prefers_regular_name_over_event_suffix() {
        let regular = HashSet::from([("Pool".to_string(), "foo_event".to_string())]);
        let event = HashSet::from([("Pool".to_string(), "foo".to_string())]);

        let kind =
            classify_live_retry_call_artifact("Pool", "foo_event", &regular, &event).unwrap();

        assert_eq!(kind, LiveRetryCallArtifactKind::Regular);
    }

    #[test]
    fn live_retry_call_artifact_still_recognizes_event_triggered_suffix() {
        let regular = HashSet::new();
        let event = HashSet::from([("Pool".to_string(), "foo".to_string())]);

        let kind =
            classify_live_retry_call_artifact("Pool", "foo_event", &regular, &event).unwrap();

        assert_eq!(
            kind,
            LiveRetryCallArtifactKind::EventTriggered {
                base_name: "foo".to_string()
            }
        );
    }
}
