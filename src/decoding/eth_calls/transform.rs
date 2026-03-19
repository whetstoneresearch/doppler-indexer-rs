//! Transform helpers: build_result_map, convert_to_transform_call.

use std::collections::HashMap;

use super::types::{DecodedCallRecord, DecodedEventCallRecord};
use crate::transformations::DecodedCall as TransformDecodedCall;
use crate::types::config::eth_call::EvmType;
use crate::types::decoded::DecodedValue;

pub fn build_result_map(
    value: &DecodedValue,
    output_type: &EvmType,
    function_name: &str,
) -> HashMap<String, DecodedValue> {
    let mut result = HashMap::new();
    match output_type {
        EvmType::Named { name, .. } => {
            result.insert(name.clone(), value.clone());
        }
        EvmType::NamedTuple(fields) => {
            if let DecodedValue::NamedTuple(named_values) = value {
                for ((field_name, field_type), (_, val)) in
                    fields.iter().zip(named_values.iter())
                {
                    insert_flattened(&mut result, field_name, field_type, val);
                }
            }
        }
        _ => {
            result.insert(function_name.to_string(), value.clone());
        }
    }
    result
}

/// Like `build_result_map`, but prefixes NamedTuple field keys with the function name.
/// This is needed when merging multiple function results into a single "once" DecodedCall,
/// so that keys like `totalSupply` become `getAssetData.totalSupply` — matching the parquet
/// column naming convention (`{function_name}.{field_name}`).
pub fn build_result_map_for_merge(
    value: &DecodedValue,
    output_type: &EvmType,
    function_name: &str,
) -> HashMap<String, DecodedValue> {
    let base = build_result_map(value, output_type, function_name);
    match output_type {
        EvmType::NamedTuple(_) => base
            .into_iter()
            .map(|(k, v)| (format!("{}.{}", function_name, k), v))
            .collect(),
        _ => base,
    }
}

/// Recursively flatten nested NamedTuples into dot-notation keys.
fn insert_flattened(
    result: &mut HashMap<String, DecodedValue>,
    prefix: &str,
    field_type: &EvmType,
    value: &DecodedValue,
) {
    match (field_type, value) {
        (EvmType::NamedTuple(fields), DecodedValue::NamedTuple(named_values)) => {
            for ((child_name, child_type), (_, child_val)) in
                fields.iter().zip(named_values.iter())
            {
                let key = format!("{}.{}", prefix, child_name);
                insert_flattened(result, &key, child_type, child_val);
            }
        }
        (EvmType::Named { inner, .. }, _) => {
            insert_flattened(result, prefix, inner, value);
        }
        _ => {
            result.insert(prefix.to_string(), value.clone());
        }
    }
}

/// Convert a DecodedCallRecord to a TransformDecodedCall
pub(super) fn convert_to_transform_call(
    record: &DecodedCallRecord,
    source_name: &str,
    function_name: &str,
    output_type: &EvmType,
) -> TransformDecodedCall {
    TransformDecodedCall {
        block_number: record.block_number,
        block_timestamp: record.block_timestamp,
        contract_address: record.contract_address,
        source_name: source_name.to_string(),
        function_name: function_name.to_string(),
        trigger_log_index: None,
        result: build_result_map(&record.decoded_value, output_type, function_name),
    }
}

/// Convert a DecodedEventCallRecord to a TransformDecodedCall
pub(super) fn convert_event_call_to_transform_call(
    record: &DecodedEventCallRecord,
    source_name: &str,
    function_name: &str,
    output_type: &EvmType,
) -> TransformDecodedCall {
    TransformDecodedCall {
        block_number: record.block_number,
        block_timestamp: record.block_timestamp,
        contract_address: record.target_address,
        source_name: source_name.to_string(),
        function_name: function_name.to_string(),
        trigger_log_index: Some(record.log_index),
        result: build_result_map(&record.decoded_value, output_type, function_name),
    }
}
