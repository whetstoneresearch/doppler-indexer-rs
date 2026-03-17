//! Config builder for decoded eth_call configurations.

use super::types::{CallDecodeConfig, EventCallDecodeConfig};
use crate::types::config::contract::Contracts;

pub fn build_decode_configs(
    contracts: &Contracts,
) -> (
    Vec<CallDecodeConfig>,
    Vec<CallDecodeConfig>,
    Vec<EventCallDecodeConfig>,
) {
    let mut regular = Vec::new();
    let mut once = Vec::new();
    let mut event = Vec::new();

    for (contract_name, contract) in contracts {
        let start_block = contract.start_block.map(|u| u.to::<u64>());

        if let Some(calls) = &contract.calls {
            for call in calls {
                let function_name = call
                    .function
                    .split('(')
                    .next()
                    .unwrap_or(&call.function)
                    .to_string();

                if call.frequency.is_once() {
                    once.push(CallDecodeConfig {
                        contract_name: contract_name.clone(),
                        function_name,
                        output_type: call.output_type.clone(),
                        _is_once: true,
                        start_block,
                    });
                } else if call.frequency.is_on_events() {
                    event.push(EventCallDecodeConfig {
                        contract_name: contract_name.clone(),
                        function_name,
                        output_type: call.output_type.clone(),
                        start_block,
                    });
                } else {
                    regular.push(CallDecodeConfig {
                        contract_name: contract_name.clone(),
                        function_name,
                        output_type: call.output_type.clone(),
                        _is_once: false,
                        start_block,
                    });
                }
            }
        }

        // Factory calls - use None for start_block (they use discovery block)
        if let Some(factories) = &contract.factories {
            for factory in factories {
                if let Some(calls) = &factory.calls {
                    for call in calls {
                        let function_name = call
                            .function
                            .split('(')
                            .next()
                            .unwrap_or(&call.function)
                            .to_string();

                        if call.frequency.is_once() {
                            once.push(CallDecodeConfig {
                                contract_name: factory.collection.clone(),
                                function_name,
                                output_type: call.output_type.clone(),
                                _is_once: true,
                                start_block: None,
                            });
                        } else if call.frequency.is_on_events() {
                            event.push(EventCallDecodeConfig {
                                contract_name: factory.collection.clone(),
                                function_name,
                                output_type: call.output_type.clone(),
                                start_block: None,
                            });
                        } else {
                            regular.push(CallDecodeConfig {
                                contract_name: factory.collection.clone(),
                                function_name,
                                output_type: call.output_type.clone(),
                                _is_once: false,
                                start_block: None,
                            });
                        }
                    }
                }
            }
        }
    }

    (regular, once, event)
}

