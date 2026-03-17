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
                for ((field_name, _), (_, val)) in fields.iter().zip(named_values.iter()) {
                    result.insert(field_name.clone(), val.clone());
                }
            }
        }
        _ => {
            result.insert(function_name.to_string(), value.clone());
        }
    }
    result
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
