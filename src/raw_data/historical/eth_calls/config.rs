//! Config builders for eth_call collection: build_call_configs,
//! build_once_call_configs, build_factory_once_call_configs, compute_function_selector.

use std::collections::HashMap;

use alloy::dyn_abi::DynSolValue;
use alloy::primitives::Bytes;

use super::helpers::parse_function_name;
use super::types::{
    CallConfig, EncodedParam, EthCallCollectionError, OnceCallConfig, ParamCombinations,
};
use crate::types::config::contract::{AddressOrAddresses, Contracts};
use crate::types::config::eth_call::{
    encode_call_with_params, EthCallConfig, ParamConfig, ParamError,
};

pub(crate) fn generate_param_combinations(
    params: &[ParamConfig],
) -> Result<ParamCombinations, ParamError> {
    if params.is_empty() {
        return Ok(vec![vec![]]);
    }

    let mut result = vec![vec![]];

    for param in params {
        // Only Static params are supported for block-based frequency calls
        // FromEvent and SelfAddress params are for on_events frequency only
        let values = match param.values() {
            Some(values) => values,
            None => {
                return Err(ParamError::TypeMismatch {
                    expected: "static param with values".to_string(),
                    got: "from_event or source param (only valid for on_events frequency)"
                        .to_string(),
                });
            }
        };

        let param_type = param.param_type();
        let mut new_result = Vec::new();
        for existing in &result {
            for value in values {
                let mut combo = existing.clone();
                let dyn_val = param_type.parse_value(value)?;
                let encoded = dyn_val.abi_encode();
                combo.push((param_type.clone(), value.clone(), encoded));
                new_result.push(combo);
            }
        }
        result = new_result;
    }

    Ok(result)
}

pub fn build_call_configs(
    contracts: &Contracts,
) -> Result<Vec<CallConfig>, EthCallCollectionError> {
    let mut configs = Vec::new();

    for (contract_name, contract) in contracts {
        let default_addresses = match &contract.address {
            AddressOrAddresses::Single(addr) => vec![*addr],
            AddressOrAddresses::Multiple(addrs) => addrs.clone(),
        };
        let start_block = contract.start_block.map(|u| u.to::<u64>());

        if let Some(calls) = &contract.calls {
            for call in calls {
                // Skip once and on_events calls - they're handled separately
                if call.frequency.is_once() || call.frequency.is_on_events() {
                    continue;
                }

                // Resolve target addresses: use target override if specified, otherwise contract addresses
                let addresses = if let Some(target) = &call.target {
                    match target.resolve_all(contracts) {
                        Some(addrs) => addrs,
                        None => {
                            tracing::warn!(
                                "Could not resolve target for call {} on contract {}, skipping",
                                call.function,
                                contract_name
                            );
                            continue;
                        }
                    }
                } else {
                    default_addresses.clone()
                };

                let selector = compute_function_selector(&call.function);
                let function_name = parse_function_name(&call.function);

                let param_combinations = generate_param_combinations(&call.params)?;

                for address in &addresses {
                    for param_combo in &param_combinations {
                        let dyn_values: Vec<DynSolValue> = param_combo
                            .iter()
                            .map(|(param_type, value, _): &EncodedParam| {
                                param_type.parse_value(value)
                            })
                            .collect::<Result<_, _>>()?;

                        let encoded_calldata = encode_call_with_params(selector, &dyn_values);

                        let param_values: Vec<Vec<u8>> = param_combo
                            .iter()
                            .map(|(_, _, encoded): &EncodedParam| encoded.clone())
                            .collect();

                        configs.push(CallConfig {
                            contract_name: contract_name.clone(),
                            address: *address,
                            function_name: function_name.clone(),
                            encoded_calldata,
                            param_values,
                            frequency: call.frequency.clone(),
                            start_block,
                        });
                    }
                }
            }
        }
    }

    Ok(configs)
}

pub fn build_once_call_configs(contracts: &Contracts) -> HashMap<String, Vec<OnceCallConfig>> {
    let mut configs: HashMap<String, Vec<OnceCallConfig>> = HashMap::new();

    for (contract_name, contract) in contracts {
        let start_block = contract.start_block.map(|u| u.to::<u64>());

        if let Some(calls) = &contract.calls {
            for call in calls {
                if call.frequency.is_once() {
                    let selector = compute_function_selector(&call.function);
                    let function_name = parse_function_name(&call.function);

                    // Check if call has self-address params (requires dynamic encoding per address)
                    let (preencoded_calldata, params) = if call.has_self_address_param() {
                        // Need to encode dynamically per address
                        (None, call.params.clone())
                    } else if call.params.is_empty() {
                        // No params - just the selector
                        (Some(Bytes::copy_from_slice(&selector)), vec![])
                    } else {
                        // Static params only - pre-encode now
                        // Note: This uses first value from each static param
                        let mut dyn_values = Vec::new();
                        let mut all_static = true;
                        for param in &call.params {
                            match param {
                                ParamConfig::Static { param_type, values } => {
                                    if let Some(value) = values.first() {
                                        if let Ok(dyn_val) = param_type.parse_value(value) {
                                            dyn_values.push(dyn_val);
                                        } else {
                                            all_static = false;
                                            break;
                                        }
                                    } else {
                                        all_static = false;
                                        break;
                                    }
                                }
                                _ => {
                                    all_static = false;
                                    break;
                                }
                            }
                        }
                        if all_static {
                            (Some(encode_call_with_params(selector, &dyn_values)), vec![])
                        } else {
                            // Fallback: store params for dynamic encoding
                            (None, call.params.clone())
                        }
                    };

                    // Resolve target override addresses if specified
                    let target_addresses = call.target.as_ref().and_then(|t| {
                        let resolved = t.resolve_all(contracts);
                        if resolved.is_none() {
                            tracing::warn!(
                                "Could not resolve target for once call {} on contract {}, will use contract addresses",
                                call.function, contract_name
                            );
                        }
                        resolved
                    });

                    configs
                        .entry(contract_name.clone())
                        .or_default()
                        .push(OnceCallConfig {
                            function_name,
                            function_selector: selector,
                            preencoded_calldata,
                            params,
                            target_addresses,
                            start_block,
                        });
                }
            }
        }
    }

    configs
}

pub fn build_factory_once_call_configs(
    factory_call_configs: &HashMap<String, Vec<EthCallConfig>>,
    contracts: &Contracts,
) -> HashMap<String, Vec<OnceCallConfig>> {
    let mut configs: HashMap<String, Vec<OnceCallConfig>> = HashMap::new();

    for (collection_name, call_configs) in factory_call_configs {
        for call in call_configs {
            if call.frequency.is_once() {
                let selector = compute_function_selector(&call.function);
                let function_name = parse_function_name(&call.function);

                // Resolve target override if specified (resolves to first address only for factory calls)
                let target_addresses = call.target.as_ref().and_then(|t| {
                    let resolved = t.resolve(contracts);
                    if resolved.is_none() {
                        tracing::warn!(
                            "Could not resolve target for factory once call {} on collection {}, will use factory addresses",
                            call.function, collection_name
                        );
                    }
                    // For factory once calls, we resolve to a single target address
                    resolved.map(|addr| vec![addr])
                });

                // Check if call has self-address params (requires dynamic encoding per address)
                let (preencoded_calldata, params) = if call.has_self_address_param() {
                    // Need to encode dynamically per address
                    (None, call.params.clone())
                } else if call.params.is_empty() {
                    // No params - just the selector
                    (Some(Bytes::copy_from_slice(&selector)), vec![])
                } else {
                    // Static params only - pre-encode now
                    let mut dyn_values = Vec::new();
                    let mut all_static = true;
                    for param in &call.params {
                        match param {
                            ParamConfig::Static { param_type, values } => {
                                if let Some(value) = values.first() {
                                    if let Ok(dyn_val) = param_type.parse_value(value) {
                                        dyn_values.push(dyn_val);
                                    } else {
                                        all_static = false;
                                        break;
                                    }
                                } else {
                                    all_static = false;
                                    break;
                                }
                            }
                            _ => {
                                all_static = false;
                                break;
                            }
                        }
                    }
                    if all_static {
                        (Some(encode_call_with_params(selector, &dyn_values)), vec![])
                    } else {
                        // Fallback: store params for dynamic encoding
                        (None, call.params.clone())
                    }
                };

                configs
                    .entry(collection_name.clone())
                    .or_default()
                    .push(OnceCallConfig {
                        function_name,
                        function_selector: selector,
                        preencoded_calldata,
                        params,
                        target_addresses,
                        // Factory calls use discovery block, not start_block
                        start_block: None,
                    });
            }
        }
    }

    configs
}

pub(crate) fn compute_function_selector(signature: &str) -> [u8; 4] {
    use alloy::primitives::keccak256;
    let hash = keccak256(signature.as_bytes());
    let mut selector = [0u8; 4];
    selector.copy_from_slice(&hash[0..4]);
    selector
}
