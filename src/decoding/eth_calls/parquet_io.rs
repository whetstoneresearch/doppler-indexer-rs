//! Decoded parquet writers/readers and arrow array builders.

use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, FixedSizeBinaryArray, FixedSizeBinaryBuilder,
    Int16Array, Int32Array, Int64Array, Int8Array, StringArray, StructArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use super::types::{
    CallDecodeConfig, DecodedCallRecord, DecodedEventCallRecord, DecodedOnceRecord,
    EthCallDecodingError,
};
use crate::types::config::eth_call::EvmType;
use crate::types::decoded::DecodedValue;

pub(super) fn write_decoded_calls_to_parquet(
    records: &[DecodedCallRecord],
    output_type: &EvmType,
    output_path: &Path,
) -> Result<(), EthCallDecodingError> {
    // Build schema based on output type
    let mut fields = vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("block_timestamp", DataType::UInt64, false),
        Field::new("address", DataType::FixedSizeBinary(20), false),
    ];

    // Add value fields based on output type
    match output_type {
        EvmType::Named { name, inner } => {
            // Named single value: use the name as column name
            fields.push(Field::new(name, inner.to_arrow_type(), true));
        }
        EvmType::NamedTuple(tuple_fields) => {
            // Named tuple: flatten nested tuples into dot-notation columns
            for (field_name, field_type) in tuple_fields {
                add_flattened_fields(&mut fields, field_name, field_type);
            }
        }
        _ => {
            // Simple type: use "decoded_value"
            fields.push(Field::new(
                "decoded_value",
                output_type.to_arrow_type(),
                true,
            ));
        }
    }

    let schema = Arc::new(Schema::new(fields));
    let mut arrays: Vec<ArrayRef> = Vec::new();

    // block_number
    let arr: UInt64Array = records.iter().map(|r| Some(r.block_number)).collect();
    arrays.push(Arc::new(arr));

    // block_timestamp
    let arr: UInt64Array = records.iter().map(|r| Some(r.block_timestamp)).collect();
    arrays.push(Arc::new(arr));

    // contract_address
    if records.is_empty() {
        arrays.push(Arc::new(FixedSizeBinaryBuilder::new(20).finish()));
    } else {
        let arr = FixedSizeBinaryArray::try_from_iter(
            records.iter().map(|r| r.contract_address.as_slice()),
        )?;
        arrays.push(Arc::new(arr));
    }

    // Add value arrays based on output type
    match output_type {
        EvmType::Named { inner, .. } => {
            // Named single value: build array using inner type
            let value_array = build_value_array(records, inner)?;
            arrays.push(value_array);
        }
        EvmType::NamedTuple(tuple_fields) => {
            // Named tuple: flatten nested tuples into leaf arrays
            let leaves = collect_flat_leaves(tuple_fields);
            for (_, access_path, leaf_type) in &leaves {
                let values: Vec<Option<&DecodedValue>> = records
                    .iter()
                    .map(|r| extract_nested_value(&r.decoded_value, access_path))
                    .collect();
                let arr = build_array_from_decoded_values(&values, leaf_type)?;
                arrays.push(arr);
            }
        }
        _ => {
            // Simple type
            let value_array = build_value_array(records, output_type)?;
            arrays.push(value_array);
        }
    }

    // Write to parquet
    let batch = RecordBatch::try_new(schema.clone(), arrays)?;

    let file = File::create(output_path)?;
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

/// Write decoded event-triggered calls to parquet
pub(super) fn write_decoded_event_calls_to_parquet(
    records: &[DecodedEventCallRecord],
    output_type: &EvmType,
    output_path: &Path,
) -> Result<(), EthCallDecodingError> {
    // Build schema based on output type - includes log_index for event-triggered calls
    let mut fields = vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("block_timestamp", DataType::UInt64, false),
        Field::new("log_index", DataType::UInt32, false),
        Field::new("address", DataType::FixedSizeBinary(20), false),
    ];

    // Add value fields based on output type
    match output_type {
        EvmType::Named { name, inner } => {
            fields.push(Field::new(name, inner.to_arrow_type(), true));
        }
        EvmType::NamedTuple(tuple_fields) => {
            for (field_name, field_type) in tuple_fields {
                add_flattened_fields(&mut fields, field_name, field_type);
            }
        }
        _ => {
            fields.push(Field::new(
                "decoded_value",
                output_type.to_arrow_type(),
                true,
            ));
        }
    }

    let schema = Arc::new(Schema::new(fields));
    let mut arrays: Vec<ArrayRef> = Vec::new();

    // block_number
    let arr: UInt64Array = records.iter().map(|r| Some(r.block_number)).collect();
    arrays.push(Arc::new(arr));

    // block_timestamp
    let arr: UInt64Array = records.iter().map(|r| Some(r.block_timestamp)).collect();
    arrays.push(Arc::new(arr));

    // log_index
    let arr: UInt32Array = records.iter().map(|r| Some(r.log_index)).collect();
    arrays.push(Arc::new(arr));

    // target_address
    if records.is_empty() {
        arrays.push(Arc::new(FixedSizeBinaryBuilder::new(20).finish()));
    } else {
        let arr = FixedSizeBinaryArray::try_from_iter(
            records.iter().map(|r| r.target_address.as_slice()),
        )?;
        arrays.push(Arc::new(arr));
    }

    // Add value arrays based on output type
    match output_type {
        EvmType::Named { inner, .. } => {
            let value_array = build_event_value_array(records, inner)?;
            arrays.push(value_array);
        }
        EvmType::NamedTuple(tuple_fields) => {
            let leaves = collect_flat_leaves(tuple_fields);
            for (_, access_path, leaf_type) in &leaves {
                let values: Vec<Option<&DecodedValue>> = records
                    .iter()
                    .map(|r| extract_nested_value(&r.decoded_value, access_path))
                    .collect();
                let arr = build_array_from_decoded_values(&values, leaf_type)?;
                arrays.push(arr);
            }
        }
        _ => {
            let value_array = build_event_value_array(records, output_type)?;
            arrays.push(value_array);
        }
    }

    // Write to parquet
    let batch = RecordBatch::try_new(schema.clone(), arrays)?;

    let file = File::create(output_path)?;
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

/// Build an Arrow array for event call decoded values
pub(super) fn build_event_value_array(
    records: &[DecodedEventCallRecord],
    output_type: &EvmType,
) -> Result<ArrayRef, EthCallDecodingError> {
    match output_type {
        EvmType::Address => {
            if records.is_empty() {
                Ok(Arc::new(FixedSizeBinaryBuilder::new(20).finish()))
            } else {
                let arr = FixedSizeBinaryArray::try_from_iter(records.iter().map(|r| {
                    match &r.decoded_value {
                        DecodedValue::Address(addr) => addr.as_slice(),
                        _ => &[0u8; 20][..],
                    }
                }))?;
                Ok(Arc::new(arr))
            }
        }
        EvmType::Uint8 => {
            let arr: UInt8Array = records
                .iter()
                .map(|r| match &r.decoded_value {
                    DecodedValue::Uint8(v) => Some(*v),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint64 => {
            let arr: UInt64Array = records
                .iter()
                .map(|r| match &r.decoded_value {
                    DecodedValue::Uint64(v) => Some(*v),
                    DecodedValue::Uint256(v) => (*v).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint32 | EvmType::Uint24 => {
            let arr: UInt32Array = records
                .iter()
                .map(|r| match &r.decoded_value {
                    DecodedValue::Uint64(v) => (*v).try_into().ok(),
                    DecodedValue::Uint256(v) => (*v).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint16 => {
            let arr: UInt16Array = records
                .iter()
                .map(|r| match &r.decoded_value {
                    DecodedValue::Uint64(v) => (*v).try_into().ok(),
                    DecodedValue::Uint256(v) => (*v).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint256
        | EvmType::Uint160
        | EvmType::Uint128
        | EvmType::Uint96
        | EvmType::Uint80 => {
            let arr: StringArray = records
                .iter()
                .map(|r| match &r.decoded_value {
                    DecodedValue::Uint256(v) => Some(v.to_string()),
                    DecodedValue::Uint64(v) => Some(v.to_string()),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int8 => {
            let arr: Int8Array = records
                .iter()
                .map(|r| match &r.decoded_value {
                    DecodedValue::Int8(v) => Some(*v),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int64 => {
            let arr: Int64Array = records
                .iter()
                .map(|r| match &r.decoded_value {
                    DecodedValue::Int64(v) => Some(*v),
                    DecodedValue::Int256(v) => (*v).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int32 | EvmType::Int24 => {
            let arr: Int32Array = records
                .iter()
                .map(|r| match &r.decoded_value {
                    DecodedValue::Int64(v) => (*v).try_into().ok(),
                    DecodedValue::Int256(v) => (*v).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int16 => {
            let arr: Int16Array = records
                .iter()
                .map(|r| match &r.decoded_value {
                    DecodedValue::Int64(v) => (*v).try_into().ok(),
                    DecodedValue::Int256(v) => (*v).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int256 | EvmType::Int128 => {
            let arr: StringArray = records
                .iter()
                .map(|r| match &r.decoded_value {
                    DecodedValue::Int256(v) => Some(v.to_string()),
                    DecodedValue::Int64(v) => Some(v.to_string()),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Bool => {
            let arr: BooleanArray = records
                .iter()
                .map(|r| match &r.decoded_value {
                    DecodedValue::Bool(v) => Some(*v),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Bytes32 => {
            if records.is_empty() {
                Ok(Arc::new(FixedSizeBinaryBuilder::new(32).finish()))
            } else {
                let arr = FixedSizeBinaryArray::try_from_iter(records.iter().map(|r| {
                    match &r.decoded_value {
                        DecodedValue::Bytes32(b) => b.as_slice(),
                        _ => &[0u8; 32][..],
                    }
                }))?;
                Ok(Arc::new(arr))
            }
        }
        EvmType::String => {
            let arr: StringArray = records
                .iter()
                .map(|r| match &r.decoded_value {
                    DecodedValue::String(s) => Some(s.as_str()),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Bytes => {
            let arr: BinaryArray = records
                .iter()
                .map(|r| match &r.decoded_value {
                    DecodedValue::Bytes(b) => Some(b.as_slice()),
                    DecodedValue::Bytes32(b) => Some(b.as_slice()),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Named { inner, .. } => build_event_value_array(records, inner),
        EvmType::NamedTuple(_) => Err(EthCallDecodingError::Decode(
            "NamedTuple should use build_event_tuple_field_array".to_string(),
        )),
        EvmType::UnnamedTuple(_) => Err(EthCallDecodingError::Decode(
            "UnnamedTuple should use specialized handling".to_string(),
        )),
        EvmType::Array(inner) => build_event_array_value_array(records, inner),
    }
}

/// Build an Arrow ListArray for array-typed decoded values in event calls
pub(super) fn build_event_array_value_array(
    records: &[DecodedEventCallRecord],
    inner_type: &EvmType,
) -> Result<ArrayRef, EthCallDecodingError> {
    use arrow::array::{ListBuilder, StringBuilder};
    use arrow::datatypes::Fields;

    // Extract array elements from each record
    let arrays: Vec<Option<&Vec<DecodedValue>>> = records
        .iter()
        .map(|r| match &r.decoded_value {
            DecodedValue::Array(arr) => Some(arr),
            _ => None,
        })
        .collect();

    // Build ListArray based on inner type
    match inner_type {
        EvmType::NamedTuple(_) | EvmType::UnnamedTuple(_) => {
            // For tuple arrays, build a List of Structs
            let (field_names, field_types): (Vec<String>, Vec<&EvmType>) = match inner_type {
                EvmType::NamedTuple(fields) => {
                    let names: Vec<String> = fields.iter().map(|(n, _)| n.clone()).collect();
                    let types: Vec<&EvmType> = fields.iter().map(|(_, t)| t.as_ref()).collect();
                    (names, types)
                }
                EvmType::UnnamedTuple(fields) => {
                    let names: Vec<String> = (0..fields.len()).map(|i| i.to_string()).collect();
                    let types: Vec<&EvmType> = fields.iter().collect();
                    (names, types)
                }
                _ => unreachable!(),
            };

            let arrow_fields: Vec<Field> = field_names
                .iter()
                .zip(field_types.iter())
                .map(|(name, ty)| Field::new(name, ty.to_arrow_type(), true))
                .collect();
            let struct_fields: Fields = arrow_fields.clone().into();

            // Build struct arrays for each element in each record's array
            let mut all_struct_arrays: Vec<arrow::array::StructArray> = Vec::new();
            let mut offsets: Vec<i32> = vec![0];
            let mut current_offset: i32 = 0;

            for arr_opt in &arrays {
                if let Some(arr) = arr_opt {
                    for elem in arr.iter() {
                        let struct_arr =
                            build_decoded_value_struct(elem, &field_names, &field_types)?;
                        all_struct_arrays.push(struct_arr);
                        current_offset += 1;
                    }
                }
                offsets.push(current_offset);
            }

            // Concatenate all struct arrays
            if all_struct_arrays.is_empty() {
                // Empty list - create struct array with 0 length
                let empty_struct = StructArray::new_null(struct_fields.clone(), 0);
                let list_arr = arrow::array::ListArray::try_new(
                    Arc::new(Field::new("item", DataType::Struct(struct_fields), true)),
                    arrow::buffer::OffsetBuffer::new(arrow::buffer::ScalarBuffer::from(offsets)),
                    Arc::new(empty_struct),
                    None,
                )?;
                Ok(Arc::new(list_arr))
            } else {
                let struct_refs: Vec<&dyn Array> =
                    all_struct_arrays.iter().map(|a| a as &dyn Array).collect();
                let concatenated = arrow::compute::concat(&struct_refs)?;
                let list_arr = arrow::array::ListArray::try_new(
                    Arc::new(Field::new("item", DataType::Struct(struct_fields), true)),
                    arrow::buffer::OffsetBuffer::new(arrow::buffer::ScalarBuffer::from(offsets)),
                    concatenated,
                    None,
                )?;
                Ok(Arc::new(list_arr))
            }
        }
        EvmType::Address => {
            let mut builder = ListBuilder::new(FixedSizeBinaryBuilder::new(20));
            for arr_opt in &arrays {
                if let Some(arr) = arr_opt {
                    for elem in arr.iter() {
                        if let DecodedValue::Address(addr) = elem {
                            builder.values().append_value(addr)?;
                        } else {
                            builder.values().append_value([0u8; 20])?;
                        }
                    }
                    builder.append(true);
                } else {
                    builder.append(false);
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        EvmType::Uint256
        | EvmType::Uint160
        | EvmType::Uint128
        | EvmType::Uint96
        | EvmType::Uint80 => {
            let mut builder = ListBuilder::new(StringBuilder::new());
            for arr_opt in &arrays {
                if let Some(arr) = arr_opt {
                    for elem in arr.iter() {
                        match elem {
                            DecodedValue::Uint256(v) => {
                                builder.values().append_value(v.to_string())
                            }
                            DecodedValue::Uint64(v) => builder.values().append_value(v.to_string()),
                            _ => builder.values().append_null(),
                        }
                    }
                    builder.append(true);
                } else {
                    builder.append(false);
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => Err(EthCallDecodingError::Decode(format!(
            "Unsupported array inner type for event calls: {:?}",
            inner_type
        ))),
    }
}

// ─── Flat leaf helpers for nested NamedTuple flattening ─────────────

/// Add flattened fields to schema, recursing into nested NamedTuples with dot-notation.
fn add_flattened_fields(fields: &mut Vec<Field>, prefix: &str, field_type: &EvmType) {
    match field_type {
        EvmType::NamedTuple(nested_fields) => {
            for (child_name, child_type) in nested_fields {
                add_flattened_fields(fields, &format!("{}.{}", prefix, child_name), child_type);
            }
        }
        EvmType::UnnamedTuple(nested_fields) => {
            for (idx, child_type) in nested_fields.iter().enumerate() {
                add_flattened_fields(fields, &format!("{}.{}", prefix, idx), child_type);
            }
        }
        EvmType::Named { inner, .. } => {
            add_flattened_fields(fields, prefix, inner);
        }
        other => {
            fields.push(Field::new(prefix, other.to_arrow_type(), true));
        }
    }
}

/// Collect flat leaf fields from a NamedTuple type.
/// Returns `(column_name, access_path, leaf_type)` for each leaf.
fn collect_flat_leaves(
    tuple_fields: &[(String, Box<EvmType>)],
) -> Vec<(String, Vec<usize>, EvmType)> {
    let mut result = Vec::new();
    for (idx, (name, ty)) in tuple_fields.iter().enumerate() {
        collect_leaf(name, &[idx], ty, &mut result);
    }
    result
}

fn collect_leaf(
    prefix: &str,
    path: &[usize],
    field_type: &EvmType,
    result: &mut Vec<(String, Vec<usize>, EvmType)>,
) {
    match field_type {
        EvmType::NamedTuple(nested_fields) => {
            for (nested_idx, (nested_name, nested_type)) in nested_fields.iter().enumerate() {
                let mut new_path = path.to_vec();
                new_path.push(nested_idx);
                collect_leaf(
                    &format!("{}.{}", prefix, nested_name),
                    &new_path,
                    nested_type,
                    result,
                );
            }
        }
        EvmType::UnnamedTuple(nested_fields) => {
            for (nested_idx, nested_type) in nested_fields.iter().enumerate() {
                let mut new_path = path.to_vec();
                new_path.push(nested_idx);
                collect_leaf(
                    &format!("{}.{}", prefix, nested_idx),
                    &new_path,
                    nested_type,
                    result,
                );
            }
        }
        EvmType::Named { inner, .. } => {
            collect_leaf(prefix, path, inner, result);
        }
        other => {
            result.push((prefix.to_string(), path.to_vec(), other.clone()));
        }
    }
}

/// Navigate nested NamedTuple/UnnamedTuple values using an index access path.
fn extract_nested_value<'a>(value: &'a DecodedValue, path: &[usize]) -> Option<&'a DecodedValue> {
    if path.is_empty() {
        return Some(value);
    }
    match value {
        DecodedValue::NamedTuple(fields) => fields
            .get(path[0])
            .and_then(|(_, v)| extract_nested_value(v, &path[1..])),
        DecodedValue::UnnamedTuple(fields) => fields
            .get(path[0])
            .and_then(|v| extract_nested_value(v, &path[1..])),
        _ => None,
    }
}

/// Build an Arrow array from extracted decoded values for a leaf type.
fn build_array_from_decoded_values(
    values: &[Option<&DecodedValue>],
    leaf_type: &EvmType,
) -> Result<ArrayRef, EthCallDecodingError> {
    match leaf_type {
        EvmType::Address => {
            if values.is_empty() {
                Ok(Arc::new(FixedSizeBinaryBuilder::new(20).finish()))
            } else {
                let arr =
                    FixedSizeBinaryArray::try_from_iter(values.iter().map(|opt| match opt {
                        Some(DecodedValue::Address(addr)) => addr.as_slice(),
                        _ => &[0u8; 20][..],
                    }))?;
                Ok(Arc::new(arr))
            }
        }
        EvmType::Uint8 => {
            let arr: UInt8Array = values
                .iter()
                .map(|opt| match opt {
                    Some(DecodedValue::Uint8(v)) => Some(*v),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint64 => {
            let arr: UInt64Array = values
                .iter()
                .map(|opt| match opt {
                    Some(DecodedValue::Uint64(v)) => Some(*v),
                    Some(DecodedValue::Uint256(v)) => (*v).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint32 | EvmType::Uint24 => {
            let arr: UInt32Array = values
                .iter()
                .map(|opt| match opt {
                    Some(DecodedValue::Uint64(v)) => (*v).try_into().ok(),
                    Some(DecodedValue::Uint256(v)) => (*v).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint16 => {
            let arr: UInt16Array = values
                .iter()
                .map(|opt| match opt {
                    Some(DecodedValue::Uint64(v)) => (*v).try_into().ok(),
                    Some(DecodedValue::Uint256(v)) => (*v).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint256
        | EvmType::Uint160
        | EvmType::Uint128
        | EvmType::Uint96
        | EvmType::Uint80 => {
            let arr: StringArray = values
                .iter()
                .map(|opt| match opt {
                    Some(DecodedValue::Uint256(v)) => Some(v.to_string()),
                    Some(DecodedValue::Uint64(v)) => Some(v.to_string()),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int8 => {
            let arr: Int8Array = values
                .iter()
                .map(|opt| match opt {
                    Some(DecodedValue::Int8(v)) => Some(*v),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int64 => {
            let arr: Int64Array = values
                .iter()
                .map(|opt| match opt {
                    Some(DecodedValue::Int64(v)) => Some(*v),
                    Some(DecodedValue::Int256(v)) => (*v).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int32 | EvmType::Int24 => {
            let arr: Int32Array = values
                .iter()
                .map(|opt| match opt {
                    Some(DecodedValue::Int64(v)) => (*v).try_into().ok(),
                    Some(DecodedValue::Int256(v)) => (*v).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int16 => {
            let arr: Int16Array = values
                .iter()
                .map(|opt| match opt {
                    Some(DecodedValue::Int64(v)) => (*v).try_into().ok(),
                    Some(DecodedValue::Int256(v)) => (*v).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int256 | EvmType::Int128 => {
            let arr: StringArray = values
                .iter()
                .map(|opt| match opt {
                    Some(DecodedValue::Int256(v)) => Some(v.to_string()),
                    Some(DecodedValue::Int64(v)) => Some(v.to_string()),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Bool => {
            let arr: BooleanArray = values
                .iter()
                .map(|opt| match opt {
                    Some(DecodedValue::Bool(v)) => Some(*v),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Bytes32 => {
            if values.is_empty() {
                Ok(Arc::new(FixedSizeBinaryBuilder::new(32).finish()))
            } else {
                let arr =
                    FixedSizeBinaryArray::try_from_iter(values.iter().map(|opt| match opt {
                        Some(DecodedValue::Bytes32(b)) => b.as_slice(),
                        _ => &[0u8; 32][..],
                    }))?;
                Ok(Arc::new(arr))
            }
        }
        EvmType::String => {
            let arr: StringArray = values
                .iter()
                .map(|opt| match opt {
                    Some(DecodedValue::String(s)) => Some(s.as_str()),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Bytes => {
            let arr: BinaryArray = values
                .iter()
                .map(|opt| match opt {
                    Some(DecodedValue::Bytes(b)) => Some(b.as_slice()),
                    Some(DecodedValue::Bytes32(b)) => Some(b.as_slice()),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Named { inner, .. } => build_array_from_decoded_values(values, inner),
        _ => Err(EthCallDecodingError::Decode(format!(
            "Unsupported leaf type in build_array_from_decoded_values: {:?}",
            leaf_type
        ))),
    }
}

/// Read existing decoded once parquet file and return record batches
pub(super) fn read_existing_decoded_once_parquet(
    path: &Path,
) -> Result<Vec<RecordBatch>, EthCallDecodingError> {
    let file = File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>()?;
    Ok(batches)
}

/// Merge new decoded columns into an existing decoded once parquet file
pub(super) fn merge_decoded_once_calls(
    output_path: &Path,
    new_records: &[DecodedOnceRecord],
    new_configs: &[&CallDecodeConfig],
) -> Result<(), EthCallDecodingError> {
    // Read existing parquet
    let existing_batches = read_existing_decoded_once_parquet(output_path)?;
    if existing_batches.is_empty() {
        // No existing data, just write new
        return write_decoded_once_calls_to_parquet(new_records, new_configs, output_path);
    }

    let existing_batch = &existing_batches[0];
    let existing_schema = existing_batch.schema();

    // Build address lookup map from new_records
    let new_records_by_address: HashMap<[u8; 20], &DecodedOnceRecord> = new_records
        .iter()
        .map(|r| (r.contract_address, r))
        .collect();

    // Extract address column from existing batch
    let address_col_idx = existing_schema
        .index_of("address")
        .map_err(|e| EthCallDecodingError::Decode(format!("Missing address column: {}", e)))?;
    let address_arr = existing_batch
        .column(address_col_idx)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| {
            EthCallDecodingError::Decode("address column is not FixedSizeBinaryArray".to_string())
        })?;

    // Build new schema with existing fields + new fields
    let mut fields: Vec<Field> = existing_schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();

    // Add new fields for each new config
    for config in new_configs {
        match &config.output_type {
            EvmType::NamedTuple(tuple_fields) => {
                for (field_name, field_type) in tuple_fields {
                    let col_name = format!("{}.{}", config.function_name, field_name);
                    if !fields.iter().any(|f| f.name() == &col_name) {
                        fields.push(Field::new(&col_name, field_type.to_arrow_type(), true));
                    }
                }
            }
            _ => {
                let col_name = config.function_name.clone();
                if !fields.iter().any(|f| f.name() == &col_name) {
                    let value_type = config.output_type.to_arrow_type();
                    fields.push(Field::new(&col_name, value_type, true));
                }
            }
        }
    }

    let mut arrays: Vec<ArrayRef> = Vec::new();

    // Build a lookup from column name -> (config, optional tuple field info)
    // This is used to fill nulls in existing columns from new decoded records.
    let mut col_config_lookup: super::types::ColumnConfigLookup<'_> = HashMap::new();
    for config in new_configs {
        match &config.output_type {
            EvmType::NamedTuple(tuple_fields) => {
                for (idx, (field_name, field_type)) in tuple_fields.iter().enumerate() {
                    let col_name = format!("{}.{}", config.function_name, field_name);
                    col_config_lookup.insert(col_name, (config, Some((idx, field_type))));
                }
            }
            _ => {
                col_config_lookup.insert(config.function_name.clone(), (config, None));
            }
        }
    }

    // Copy existing columns, filling nulls where possible from new decoded records
    for (i, field) in existing_schema
        .fields()
        .iter()
        .enumerate()
        .take(existing_batch.num_columns())
    {
        let col = existing_batch.column(i);
        let col_name = field.name().clone();

        if col.null_count() > 0 {
            if let Some((config, tuple_info)) = col_config_lookup.get(&col_name) {
                // This column has nulls and we have new decoded data — rebuild it
                let rebuilt = match tuple_info {
                    Some((field_idx, field_type)) => build_once_tuple_field_array_aligned(
                        address_arr,
                        &new_records_by_address,
                        &config.function_name,
                        *field_idx,
                        field_type,
                    )?,
                    None => build_once_value_array_aligned(
                        address_arr,
                        &new_records_by_address,
                        &config.function_name,
                        &config.output_type,
                    )?,
                };

                let old_nulls = col.null_count();
                let new_nulls = rebuilt.null_count();
                if new_nulls < old_nulls {
                    tracing::info!(
                        "Replacing column '{}' (had {} nulls, now {} nulls)",
                        col_name,
                        old_nulls,
                        new_nulls
                    );
                    // Update field type if the config's output type changed
                    if rebuilt.data_type() != col.data_type() {
                        tracing::info!(
                            "Column '{}' type changed from {:?} to {:?}",
                            col_name,
                            col.data_type(),
                            rebuilt.data_type()
                        );
                        fields[i] = Field::new(&col_name, rebuilt.data_type().clone(), true);
                    }
                    arrays.push(rebuilt);
                    continue;
                } else if old_nulls > 0 {
                    tracing::warn!(
                        "Column '{}' has {} nulls but rebuilt column still has {} nulls (all decodes failed — check output_type {:?})",
                        col_name,
                        old_nulls,
                        new_nulls,
                        config.output_type
                    );
                }
            }
        }
        arrays.push(col.clone());
    }

    // Build new columns aligned to existing addresses
    for config in new_configs {
        match &config.output_type {
            EvmType::NamedTuple(tuple_fields) => {
                for (idx, (_, field_type)) in tuple_fields.iter().enumerate() {
                    let col_name = format!("{}.{}", config.function_name, tuple_fields[idx].0);
                    if existing_schema.index_of(&col_name).is_err() {
                        let arr = build_once_tuple_field_array_aligned(
                            address_arr,
                            &new_records_by_address,
                            &config.function_name,
                            idx,
                            field_type,
                        )?;
                        arrays.push(arr);
                    }
                }
            }
            _ => {
                let col_name = config.function_name.clone();
                if existing_schema.index_of(&col_name).is_err() {
                    let arr = build_once_value_array_aligned(
                        address_arr,
                        &new_records_by_address,
                        &config.function_name,
                        &config.output_type,
                    )?;
                    arrays.push(arr);
                }
            }
        }
    }

    // Build schema after column processing (fields may have been updated during null-filling)
    let new_schema = Arc::new(Schema::new(fields));

    // Write merged parquet
    let batch = RecordBatch::try_new(new_schema.clone(), arrays)?;

    let file = File::create(output_path)?;
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(file, new_schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

/// Write decoded "once" calls to parquet
pub(super) fn write_decoded_once_calls_to_parquet(
    records: &[DecodedOnceRecord],
    configs: &[&CallDecodeConfig],
    output_path: &Path,
) -> Result<(), EthCallDecodingError> {
    let mut fields = vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("block_timestamp", DataType::UInt64, false),
        Field::new("address", DataType::FixedSizeBinary(20), false),
    ];

    // Add a field for each function result
    for config in configs {
        match &config.output_type {
            EvmType::NamedTuple(tuple_fields) => {
                // Named tuple: create a column for each field with {function}_decoded.{field_name}
                for (field_name, field_type) in tuple_fields {
                    fields.push(Field::new(
                        format!("{}.{}", config.function_name, field_name),
                        field_type.to_arrow_type(),
                        true,
                    ));
                }
            }
            _ => {
                // Simple type or named single value
                let value_type = config.output_type.to_arrow_type();
                fields.push(Field::new(
                    config.function_name.to_string(),
                    value_type,
                    true,
                ));
            }
        }
    }

    let schema = Arc::new(Schema::new(fields));
    let mut arrays: Vec<ArrayRef> = Vec::new();

    // block_number
    let arr: UInt64Array = records.iter().map(|r| Some(r.block_number)).collect();
    arrays.push(Arc::new(arr));

    // block_timestamp
    let arr: UInt64Array = records.iter().map(|r| Some(r.block_timestamp)).collect();
    arrays.push(Arc::new(arr));

    // contract_address
    if records.is_empty() {
        arrays.push(Arc::new(FixedSizeBinaryBuilder::new(20).finish()));
    } else {
        let arr = FixedSizeBinaryArray::try_from_iter(
            records.iter().map(|r| r.contract_address.as_slice()),
        )?;
        arrays.push(Arc::new(arr));
    }

    // Function result columns
    for config in configs {
        match &config.output_type {
            EvmType::NamedTuple(tuple_fields) => {
                // Named tuple: build an array for each field
                for (idx, (_, field_type)) in tuple_fields.iter().enumerate() {
                    let arr = build_once_tuple_field_array(
                        records,
                        &config.function_name,
                        idx,
                        field_type,
                    )?;
                    arrays.push(arr);
                }
            }
            _ => {
                let arr =
                    build_once_value_array(records, &config.function_name, &config.output_type)?;
                arrays.push(arr);
            }
        }
    }

    // Write to parquet
    let batch = RecordBatch::try_new(schema.clone(), arrays)?;

    let file = File::create(output_path)?;
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

/// Build an Arrow array for decoded values
pub(super) fn build_value_array(
    records: &[DecodedCallRecord],
    output_type: &EvmType,
) -> Result<ArrayRef, EthCallDecodingError> {
    match output_type {
        EvmType::Address => {
            if records.is_empty() {
                Ok(Arc::new(FixedSizeBinaryBuilder::new(20).finish()))
            } else {
                let arr = FixedSizeBinaryArray::try_from_iter(records.iter().map(|r| {
                    match &r.decoded_value {
                        DecodedValue::Address(addr) => addr.as_slice(),
                        _ => &[0u8; 20][..],
                    }
                }))?;
                Ok(Arc::new(arr))
            }
        }
        EvmType::Uint8 => {
            let arr: UInt8Array = records
                .iter()
                .map(|r| match &r.decoded_value {
                    DecodedValue::Uint8(v) => Some(*v),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint64 => {
            let arr: UInt64Array = records
                .iter()
                .map(|r| match &r.decoded_value {
                    DecodedValue::Uint64(v) => Some(*v),
                    DecodedValue::Uint256(v) => (*v).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint32 | EvmType::Uint24 => {
            let arr: UInt32Array = records
                .iter()
                .map(|r| match &r.decoded_value {
                    DecodedValue::Uint64(v) => (*v).try_into().ok(),
                    DecodedValue::Uint256(v) => (*v).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint16 => {
            let arr: UInt16Array = records
                .iter()
                .map(|r| match &r.decoded_value {
                    DecodedValue::Uint64(v) => (*v).try_into().ok(),
                    DecodedValue::Uint256(v) => (*v).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint256
        | EvmType::Uint160
        | EvmType::Uint128
        | EvmType::Uint96
        | EvmType::Uint80 => {
            let arr: StringArray = records
                .iter()
                .map(|r| match &r.decoded_value {
                    DecodedValue::Uint256(v) => Some(v.to_string()),
                    DecodedValue::Uint64(v) => Some(v.to_string()),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int8 => {
            let arr: Int8Array = records
                .iter()
                .map(|r| match &r.decoded_value {
                    DecodedValue::Int8(v) => Some(*v),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int64 => {
            let arr: Int64Array = records
                .iter()
                .map(|r| match &r.decoded_value {
                    DecodedValue::Int64(v) => Some(*v),
                    DecodedValue::Int256(v) => (*v).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int32 | EvmType::Int24 => {
            let arr: Int32Array = records
                .iter()
                .map(|r| match &r.decoded_value {
                    DecodedValue::Int64(v) => (*v).try_into().ok(),
                    DecodedValue::Int256(v) => (*v).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int16 => {
            let arr: Int16Array = records
                .iter()
                .map(|r| match &r.decoded_value {
                    DecodedValue::Int64(v) => (*v).try_into().ok(),
                    DecodedValue::Int256(v) => (*v).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int256 | EvmType::Int128 => {
            let arr: StringArray = records
                .iter()
                .map(|r| match &r.decoded_value {
                    DecodedValue::Int256(v) => Some(v.to_string()),
                    DecodedValue::Int64(v) => Some(v.to_string()),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Bool => {
            let arr: BooleanArray = records
                .iter()
                .map(|r| match &r.decoded_value {
                    DecodedValue::Bool(v) => Some(*v),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Bytes32 => {
            if records.is_empty() {
                Ok(Arc::new(FixedSizeBinaryBuilder::new(32).finish()))
            } else {
                let arr = FixedSizeBinaryArray::try_from_iter(records.iter().map(|r| {
                    match &r.decoded_value {
                        DecodedValue::Bytes32(b) => b.as_slice(),
                        _ => &[0u8; 32][..],
                    }
                }))?;
                Ok(Arc::new(arr))
            }
        }
        EvmType::String => {
            let arr: StringArray = records
                .iter()
                .map(|r| match &r.decoded_value {
                    DecodedValue::String(s) => Some(s.as_str()),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Bytes => {
            let arr: BinaryArray = records
                .iter()
                .map(|r| match &r.decoded_value {
                    DecodedValue::Bytes(b) => Some(b.as_slice()),
                    DecodedValue::Bytes32(b) => Some(b.as_slice()),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        // Named types delegate to their inner type
        EvmType::Named { inner, .. } => build_value_array(records, inner),
        // NamedTuple should not use this function directly - use build_named_tuple_arrays
        EvmType::NamedTuple(_) => Err(EthCallDecodingError::Decode(
            "NamedTuple should use build_named_tuple_arrays".to_string(),
        )),
        // UnnamedTuple and Array need special handling
        EvmType::UnnamedTuple(_) => Err(EthCallDecodingError::Decode(
            "UnnamedTuple should use specialized handling".to_string(),
        )),
        EvmType::Array(inner) => build_array_value_array(records, inner),
    }
}

/// Build an Arrow ListArray for array-typed decoded values
pub(super) fn build_array_value_array(
    records: &[DecodedCallRecord],
    inner_type: &EvmType,
) -> Result<ArrayRef, EthCallDecodingError> {
    use arrow::array::{ListBuilder, StringBuilder, StructArray};
    use arrow::datatypes::Fields;

    // Extract array elements from each record
    let arrays: Vec<Option<&Vec<DecodedValue>>> = records
        .iter()
        .map(|r| match &r.decoded_value {
            DecodedValue::Array(arr) => Some(arr),
            _ => None,
        })
        .collect();

    // Build ListArray based on inner type
    match inner_type {
        EvmType::NamedTuple(_) | EvmType::UnnamedTuple(_) => {
            // For tuple arrays, build a List of Structs
            let (field_names, field_types): (Vec<String>, Vec<&EvmType>) = match inner_type {
                EvmType::NamedTuple(fields) => {
                    let names: Vec<String> = fields.iter().map(|(n, _)| n.clone()).collect();
                    let types: Vec<&EvmType> = fields.iter().map(|(_, t)| t.as_ref()).collect();
                    (names, types)
                }
                EvmType::UnnamedTuple(fields) => {
                    let names: Vec<String> = (0..fields.len()).map(|i| i.to_string()).collect();
                    let types: Vec<&EvmType> = fields.iter().collect();
                    (names, types)
                }
                _ => unreachable!(),
            };

            let arrow_fields: Vec<Field> = field_names
                .iter()
                .zip(field_types.iter())
                .map(|(name, ty)| Field::new(name, ty.to_arrow_type(), true))
                .collect();
            let struct_fields: Fields = arrow_fields.clone().into();

            // Build struct arrays for each element in each record's array
            let mut all_struct_arrays: Vec<StructArray> = Vec::new();
            let mut offsets: Vec<i32> = vec![0];
            let mut current_offset: i32 = 0;

            for arr_opt in &arrays {
                if let Some(arr) = arr_opt {
                    for elem in arr.iter() {
                        let struct_arr =
                            build_decoded_value_struct(elem, &field_names, &field_types)?;
                        all_struct_arrays.push(struct_arr);
                        current_offset += 1;
                    }
                }
                offsets.push(current_offset);
            }

            // Concatenate all struct arrays
            if all_struct_arrays.is_empty() {
                // Empty list - create struct array with 0 length
                let empty_struct = StructArray::new_null(struct_fields.clone(), 0);
                let list_arr = arrow::array::ListArray::try_new(
                    Arc::new(Field::new("item", DataType::Struct(struct_fields), true)),
                    arrow::buffer::OffsetBuffer::new(arrow::buffer::ScalarBuffer::from(offsets)),
                    Arc::new(empty_struct),
                    None,
                )?;
                Ok(Arc::new(list_arr))
            } else {
                let struct_refs: Vec<&dyn Array> =
                    all_struct_arrays.iter().map(|a| a as &dyn Array).collect();
                let concatenated = arrow::compute::concat(&struct_refs)?;
                let list_arr = arrow::array::ListArray::try_new(
                    Arc::new(Field::new("item", DataType::Struct(struct_fields), true)),
                    arrow::buffer::OffsetBuffer::new(arrow::buffer::ScalarBuffer::from(offsets)),
                    concatenated,
                    None,
                )?;
                Ok(Arc::new(list_arr))
            }
        }
        EvmType::Address => {
            let mut builder = ListBuilder::new(FixedSizeBinaryBuilder::new(20));
            for arr_opt in &arrays {
                if let Some(arr) = arr_opt {
                    for elem in arr.iter() {
                        if let DecodedValue::Address(addr) = elem {
                            builder.values().append_value(addr)?;
                        } else {
                            builder.values().append_value([0u8; 20])?;
                        }
                    }
                    builder.append(true);
                } else {
                    builder.append(false);
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        EvmType::Uint256
        | EvmType::Uint160
        | EvmType::Uint128
        | EvmType::Uint96
        | EvmType::Uint80 => {
            let mut builder = ListBuilder::new(StringBuilder::new());
            for arr_opt in &arrays {
                if let Some(arr) = arr_opt {
                    for elem in arr.iter() {
                        match elem {
                            DecodedValue::Uint256(v) => {
                                builder.values().append_value(v.to_string())
                            }
                            DecodedValue::Uint64(v) => builder.values().append_value(v.to_string()),
                            _ => builder.values().append_null(),
                        }
                    }
                    builder.append(true);
                } else {
                    builder.append(false);
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => Err(EthCallDecodingError::Decode(format!(
            "Unsupported array inner type: {:?}",
            inner_type
        ))),
    }
}

/// Build a StructArray from a single DecodedValue (NamedTuple or UnnamedTuple)
pub(super) fn build_decoded_value_struct(
    value: &DecodedValue,
    field_names: &[String],
    field_types: &[&EvmType],
) -> Result<StructArray, EthCallDecodingError> {
    use arrow::datatypes::Fields;

    let fields_vec: Vec<(&DecodedValue, &String, &EvmType)> = match value {
        DecodedValue::NamedTuple(named_fields) => named_fields
            .iter()
            .zip(field_names.iter().zip(field_types.iter()))
            .map(|((_, v), (n, t))| (v, n, *t))
            .collect(),
        DecodedValue::UnnamedTuple(unnamed_fields) => unnamed_fields
            .iter()
            .zip(field_names.iter().zip(field_types.iter()))
            .map(|(v, (n, t))| (v, n, *t))
            .collect(),
        _ => {
            return Err(EthCallDecodingError::Decode(
                "Expected tuple value for struct building".to_string(),
            ))
        }
    };

    let mut arrays: Vec<ArrayRef> = Vec::new();
    let mut arrow_fields: Vec<Field> = Vec::new();

    for (val, name, ty) in fields_vec {
        let arr = build_single_decoded_value_array(val, ty)?;
        arrays.push(arr);
        arrow_fields.push(Field::new(name, ty.to_arrow_type(), true));
    }

    let struct_fields: Fields = arrow_fields.into();
    Ok(StructArray::try_new(struct_fields, arrays, None)?)
}

/// Build an Arrow array from a single DecodedValue
pub(super) fn build_single_decoded_value_array(
    value: &DecodedValue,
    output_type: &EvmType,
) -> Result<ArrayRef, EthCallDecodingError> {
    match (value, output_type) {
        (DecodedValue::Address(addr), EvmType::Address) => {
            let arr = FixedSizeBinaryArray::try_from_iter(std::iter::once(addr.as_slice()))?;
            Ok(Arc::new(arr))
        }
        (DecodedValue::Uint8(v), EvmType::Uint8) => {
            let arr: UInt8Array = vec![Some(*v)].into_iter().collect();
            Ok(Arc::new(arr))
        }
        (DecodedValue::Uint64(v), EvmType::Uint64) => {
            let arr: UInt64Array = vec![Some(*v)].into_iter().collect();
            Ok(Arc::new(arr))
        }
        (DecodedValue::Uint64(v), EvmType::Uint32 | EvmType::Uint24) => {
            let arr: UInt32Array = vec![(*v).try_into().ok()].into_iter().collect();
            Ok(Arc::new(arr))
        }
        (DecodedValue::Uint64(v), EvmType::Uint16) => {
            let arr: UInt16Array = vec![(*v).try_into().ok()].into_iter().collect();
            Ok(Arc::new(arr))
        }
        (
            DecodedValue::Uint256(v),
            EvmType::Uint256
            | EvmType::Uint160
            | EvmType::Uint128
            | EvmType::Uint96
            | EvmType::Uint80,
        ) => {
            let arr: StringArray = vec![Some(v.to_string())].into_iter().collect();
            Ok(Arc::new(arr))
        }
        (DecodedValue::Int8(v), EvmType::Int8) => {
            let arr: Int8Array = vec![Some(*v)].into_iter().collect();
            Ok(Arc::new(arr))
        }
        (DecodedValue::Int64(v), EvmType::Int64) => {
            let arr: Int64Array = vec![Some(*v)].into_iter().collect();
            Ok(Arc::new(arr))
        }
        (DecodedValue::Int64(v), EvmType::Int32 | EvmType::Int24) => {
            let arr: Int32Array = vec![(*v).try_into().ok()].into_iter().collect();
            Ok(Arc::new(arr))
        }
        (DecodedValue::Int64(v), EvmType::Int16) => {
            let arr: Int16Array = vec![(*v).try_into().ok()].into_iter().collect();
            Ok(Arc::new(arr))
        }
        (DecodedValue::Int256(v), EvmType::Int256 | EvmType::Int128) => {
            let arr: StringArray = vec![Some(v.to_string())].into_iter().collect();
            Ok(Arc::new(arr))
        }
        (DecodedValue::Bool(v), EvmType::Bool) => {
            let arr: BooleanArray = vec![Some(*v)].into_iter().collect();
            Ok(Arc::new(arr))
        }
        (DecodedValue::Bytes32(v), EvmType::Bytes32) => {
            let arr = FixedSizeBinaryArray::try_from_iter(std::iter::once(v.as_slice()))?;
            Ok(Arc::new(arr))
        }
        (DecodedValue::String(v), EvmType::String) => {
            let arr: StringArray = vec![Some(v.as_str())].into_iter().collect();
            Ok(Arc::new(arr))
        }
        (DecodedValue::Bytes(v), EvmType::Bytes) => {
            let arr: BinaryArray = vec![Some(v.as_slice())].into_iter().collect();
            Ok(Arc::new(arr))
        }
        _ => Err(EthCallDecodingError::Decode(format!(
            "Type mismatch: value {:?} doesn't match type {:?}",
            value, output_type
        ))),
    }
}

/// Build an Arrow array for "once" decoded values
pub(super) fn build_once_value_array(
    records: &[DecodedOnceRecord],
    function_name: &str,
    output_type: &EvmType,
) -> Result<ArrayRef, EthCallDecodingError> {
    match output_type {
        EvmType::Address => {
            if records.is_empty() {
                Ok(Arc::new(FixedSizeBinaryBuilder::new(20).finish()))
            } else {
                let arr = FixedSizeBinaryArray::try_from_iter(records.iter().map(|r| {
                    match r.decoded_values.get(function_name) {
                        Some(DecodedValue::Address(addr)) => addr.as_slice(),
                        _ => &[0u8; 20][..],
                    }
                }))?;
                Ok(Arc::new(arr))
            }
        }
        EvmType::Uint8 => {
            let arr: UInt8Array = records
                .iter()
                .map(|r| match r.decoded_values.get(function_name) {
                    Some(DecodedValue::Uint8(v)) => Some(*v),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint64 => {
            let arr: UInt64Array = records
                .iter()
                .map(|r| match r.decoded_values.get(function_name) {
                    Some(DecodedValue::Uint64(v)) => Some(*v),
                    Some(DecodedValue::Uint256(v)) => (*v).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint32 | EvmType::Uint24 => {
            let arr: UInt32Array = records
                .iter()
                .map(|r| match r.decoded_values.get(function_name) {
                    Some(DecodedValue::Uint64(v)) => (*v).try_into().ok(),
                    Some(DecodedValue::Uint256(v)) => (*v).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint16 => {
            let arr: UInt16Array = records
                .iter()
                .map(|r| match r.decoded_values.get(function_name) {
                    Some(DecodedValue::Uint64(v)) => (*v).try_into().ok(),
                    Some(DecodedValue::Uint256(v)) => (*v).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint256
        | EvmType::Uint160
        | EvmType::Uint128
        | EvmType::Uint96
        | EvmType::Uint80 => {
            let arr: StringArray = records
                .iter()
                .map(|r| match r.decoded_values.get(function_name) {
                    Some(DecodedValue::Uint256(v)) => Some(v.to_string()),
                    Some(DecodedValue::Uint64(v)) => Some(v.to_string()),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int8 => {
            let arr: Int8Array = records
                .iter()
                .map(|r| match r.decoded_values.get(function_name) {
                    Some(DecodedValue::Int8(v)) => Some(*v),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int64 => {
            let arr: Int64Array = records
                .iter()
                .map(|r| match r.decoded_values.get(function_name) {
                    Some(DecodedValue::Int64(v)) => Some(*v),
                    Some(DecodedValue::Int256(v)) => (*v).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int32 | EvmType::Int24 => {
            let arr: Int32Array = records
                .iter()
                .map(|r| match r.decoded_values.get(function_name) {
                    Some(DecodedValue::Int64(v)) => (*v).try_into().ok(),
                    Some(DecodedValue::Int256(v)) => (*v).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int16 => {
            let arr: Int16Array = records
                .iter()
                .map(|r| match r.decoded_values.get(function_name) {
                    Some(DecodedValue::Int64(v)) => (*v).try_into().ok(),
                    Some(DecodedValue::Int256(v)) => (*v).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int256 | EvmType::Int128 => {
            let arr: StringArray = records
                .iter()
                .map(|r| match r.decoded_values.get(function_name) {
                    Some(DecodedValue::Int256(v)) => Some(v.to_string()),
                    Some(DecodedValue::Int64(v)) => Some(v.to_string()),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Bool => {
            let arr: BooleanArray = records
                .iter()
                .map(|r| match r.decoded_values.get(function_name) {
                    Some(DecodedValue::Bool(v)) => Some(*v),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Bytes32 => {
            if records.is_empty() {
                Ok(Arc::new(FixedSizeBinaryBuilder::new(32).finish()))
            } else {
                let arr = FixedSizeBinaryArray::try_from_iter(records.iter().map(|r| {
                    match r.decoded_values.get(function_name) {
                        Some(DecodedValue::Bytes32(b)) => b.as_slice(),
                        _ => &[0u8; 32][..],
                    }
                }))?;
                Ok(Arc::new(arr))
            }
        }
        EvmType::String => {
            let arr: StringArray = records
                .iter()
                .map(|r| match r.decoded_values.get(function_name) {
                    Some(DecodedValue::String(s)) => Some(s.as_str()),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Bytes => {
            let arr: BinaryArray = records
                .iter()
                .map(|r| match r.decoded_values.get(function_name) {
                    Some(DecodedValue::Bytes(b)) => Some(b.as_slice()),
                    Some(DecodedValue::Bytes32(b)) => Some(b.as_slice()),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        // Named types delegate to their inner type
        EvmType::Named { inner, .. } => build_once_value_array(records, function_name, inner),
        // NamedTuple should use build_once_tuple_field_array instead
        EvmType::NamedTuple(_) => Err(EthCallDecodingError::Decode(
            "NamedTuple should use build_once_tuple_field_array".to_string(),
        )),
        EvmType::UnnamedTuple(_) => Err(EthCallDecodingError::Decode(
            "UnnamedTuple not supported for once calls".to_string(),
        )),
        EvmType::Array(_) => Err(EthCallDecodingError::Decode(
            "Array not supported for once calls".to_string(),
        )),
    }
}

/// Build an Arrow array for "once" decoded values, aligned to existing addresses.
/// For each address in `address_arr`, looks up the record in `records_by_addr` and extracts the value.
/// Returns NULL for addresses not found in the lookup map.
pub(super) fn build_once_value_array_aligned(
    address_arr: &FixedSizeBinaryArray,
    records_by_addr: &HashMap<[u8; 20], &DecodedOnceRecord>,
    function_name: &str,
    output_type: &EvmType,
) -> Result<ArrayRef, EthCallDecodingError> {
    let num_rows = address_arr.len();

    // Helper to get address at index
    let get_addr = |i: usize| -> [u8; 20] { address_arr.value(i).try_into().unwrap_or([0u8; 20]) };

    match output_type {
        EvmType::Address => {
            let mut builder = FixedSizeBinaryBuilder::with_capacity(num_rows, 20);
            for i in 0..num_rows {
                let addr = get_addr(i);
                match records_by_addr
                    .get(&addr)
                    .and_then(|r| r.decoded_values.get(function_name))
                {
                    Some(DecodedValue::Address(a)) => builder.append_value(a)?,
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        EvmType::Uint8 => {
            let arr: UInt8Array = (0..num_rows)
                .map(|i| {
                    let addr = get_addr(i);
                    records_by_addr
                        .get(&addr)
                        .and_then(|r| r.decoded_values.get(function_name))
                        .and_then(|v| match v {
                            DecodedValue::Uint8(val) => Some(*val),
                            _ => None,
                        })
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint64 => {
            let arr: UInt64Array = (0..num_rows)
                .map(|i| {
                    let addr = get_addr(i);
                    records_by_addr
                        .get(&addr)
                        .and_then(|r| r.decoded_values.get(function_name))
                        .and_then(|v| match v {
                            DecodedValue::Uint64(val) => Some(*val),
                            DecodedValue::Uint256(val) => (*val).try_into().ok(),
                            _ => None,
                        })
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint32 | EvmType::Uint24 => {
            let arr: UInt32Array = (0..num_rows)
                .map(|i| {
                    let addr = get_addr(i);
                    records_by_addr
                        .get(&addr)
                        .and_then(|r| r.decoded_values.get(function_name))
                        .and_then(|v| match v {
                            DecodedValue::Uint64(val) => (*val).try_into().ok(),
                            DecodedValue::Uint256(val) => (*val).try_into().ok(),
                            _ => None,
                        })
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint16 => {
            let arr: UInt16Array = (0..num_rows)
                .map(|i| {
                    let addr = get_addr(i);
                    records_by_addr
                        .get(&addr)
                        .and_then(|r| r.decoded_values.get(function_name))
                        .and_then(|v| match v {
                            DecodedValue::Uint64(val) => (*val).try_into().ok(),
                            DecodedValue::Uint256(val) => (*val).try_into().ok(),
                            _ => None,
                        })
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint256
        | EvmType::Uint160
        | EvmType::Uint128
        | EvmType::Uint96
        | EvmType::Uint80 => {
            let arr: StringArray = (0..num_rows)
                .map(|i| {
                    let addr = get_addr(i);
                    records_by_addr
                        .get(&addr)
                        .and_then(|r| r.decoded_values.get(function_name))
                        .and_then(|v| match v {
                            DecodedValue::Uint256(val) => Some(val.to_string()),
                            DecodedValue::Uint64(val) => Some(val.to_string()),
                            _ => None,
                        })
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int8 => {
            let arr: Int8Array = (0..num_rows)
                .map(|i| {
                    let addr = get_addr(i);
                    records_by_addr
                        .get(&addr)
                        .and_then(|r| r.decoded_values.get(function_name))
                        .and_then(|v| match v {
                            DecodedValue::Int8(val) => Some(*val),
                            _ => None,
                        })
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int64 => {
            let arr: Int64Array = (0..num_rows)
                .map(|i| {
                    let addr = get_addr(i);
                    records_by_addr
                        .get(&addr)
                        .and_then(|r| r.decoded_values.get(function_name))
                        .and_then(|v| match v {
                            DecodedValue::Int64(val) => Some(*val),
                            DecodedValue::Int256(val) => (*val).try_into().ok(),
                            _ => None,
                        })
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int32 | EvmType::Int24 => {
            let arr: Int32Array = (0..num_rows)
                .map(|i| {
                    let addr = get_addr(i);
                    records_by_addr
                        .get(&addr)
                        .and_then(|r| r.decoded_values.get(function_name))
                        .and_then(|v| match v {
                            DecodedValue::Int64(val) => (*val).try_into().ok(),
                            DecodedValue::Int256(val) => (*val).try_into().ok(),
                            _ => None,
                        })
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int16 => {
            let arr: Int16Array = (0..num_rows)
                .map(|i| {
                    let addr = get_addr(i);
                    records_by_addr
                        .get(&addr)
                        .and_then(|r| r.decoded_values.get(function_name))
                        .and_then(|v| match v {
                            DecodedValue::Int64(val) => (*val).try_into().ok(),
                            DecodedValue::Int256(val) => (*val).try_into().ok(),
                            _ => None,
                        })
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int256 | EvmType::Int128 => {
            let arr: StringArray = (0..num_rows)
                .map(|i| {
                    let addr = get_addr(i);
                    records_by_addr
                        .get(&addr)
                        .and_then(|r| r.decoded_values.get(function_name))
                        .and_then(|v| match v {
                            DecodedValue::Int256(val) => Some(val.to_string()),
                            DecodedValue::Int64(val) => Some(val.to_string()),
                            _ => None,
                        })
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Bool => {
            let arr: BooleanArray = (0..num_rows)
                .map(|i| {
                    let addr = get_addr(i);
                    records_by_addr
                        .get(&addr)
                        .and_then(|r| r.decoded_values.get(function_name))
                        .and_then(|v| match v {
                            DecodedValue::Bool(val) => Some(*val),
                            _ => None,
                        })
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Bytes32 => {
            let mut builder = FixedSizeBinaryBuilder::with_capacity(num_rows, 32);
            for i in 0..num_rows {
                let addr = get_addr(i);
                match records_by_addr
                    .get(&addr)
                    .and_then(|r| r.decoded_values.get(function_name))
                {
                    Some(DecodedValue::Bytes32(b)) => builder.append_value(b)?,
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        EvmType::String => {
            let arr: StringArray = (0..num_rows)
                .map(|i| {
                    let addr = get_addr(i);
                    records_by_addr
                        .get(&addr)
                        .and_then(|r| r.decoded_values.get(function_name))
                        .and_then(|v| match v {
                            DecodedValue::String(s) => Some(s.as_str()),
                            _ => None,
                        })
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Bytes => {
            let arr: BinaryArray = (0..num_rows)
                .map(|i| {
                    let addr = get_addr(i);
                    records_by_addr
                        .get(&addr)
                        .and_then(|r| r.decoded_values.get(function_name))
                        .and_then(|v| match v {
                            DecodedValue::Bytes(b) => Some(b.as_slice()),
                            DecodedValue::Bytes32(b) => Some(b.as_slice()),
                            _ => None,
                        })
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Named { inner, .. } => {
            build_once_value_array_aligned(address_arr, records_by_addr, function_name, inner)
        }
        EvmType::NamedTuple(_) => Err(EthCallDecodingError::Decode(
            "NamedTuple should use build_once_tuple_field_array_aligned".to_string(),
        )),
        EvmType::UnnamedTuple(_) => Err(EthCallDecodingError::Decode(
            "UnnamedTuple not supported for aligned once calls".to_string(),
        )),
        EvmType::Array(_) => Err(EthCallDecodingError::Decode(
            "Array not supported for aligned once calls".to_string(),
        )),
    }
}

/// Build an Arrow array for a specific field of a named tuple from "once" calls, aligned to existing addresses.
pub(super) fn build_once_tuple_field_array_aligned(
    address_arr: &FixedSizeBinaryArray,
    records_by_addr: &HashMap<[u8; 20], &DecodedOnceRecord>,
    function_name: &str,
    field_idx: usize,
    field_type: &EvmType,
) -> Result<ArrayRef, EthCallDecodingError> {
    let num_rows = address_arr.len();

    // Helper to get address at index
    let get_addr = |i: usize| -> [u8; 20] { address_arr.value(i).try_into().unwrap_or([0u8; 20]) };

    // Helper to extract tuple field value
    let get_tuple_field = |i: usize| -> Option<&DecodedValue> {
        let addr = get_addr(i);
        records_by_addr
            .get(&addr)
            .and_then(|r| r.decoded_values.get(function_name))
            .and_then(|v| match v {
                DecodedValue::NamedTuple(fields) => fields.get(field_idx).map(|(_, val)| val),
                _ => None,
            })
    };

    match field_type {
        EvmType::Address => {
            let mut builder = FixedSizeBinaryBuilder::with_capacity(num_rows, 20);
            for i in 0..num_rows {
                match get_tuple_field(i) {
                    Some(DecodedValue::Address(a)) => builder.append_value(a)?,
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        EvmType::Uint8 => {
            let arr: UInt8Array = (0..num_rows)
                .map(|i| match get_tuple_field(i) {
                    Some(DecodedValue::Uint8(val)) => Some(*val),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint64 => {
            let arr: UInt64Array = (0..num_rows)
                .map(|i| match get_tuple_field(i) {
                    Some(DecodedValue::Uint64(val)) => Some(*val),
                    Some(DecodedValue::Uint256(val)) => (*val).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint32 | EvmType::Uint24 => {
            let arr: UInt32Array = (0..num_rows)
                .map(|i| match get_tuple_field(i) {
                    Some(DecodedValue::Uint64(val)) => (*val).try_into().ok(),
                    Some(DecodedValue::Uint256(val)) => (*val).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint16 => {
            let arr: UInt16Array = (0..num_rows)
                .map(|i| match get_tuple_field(i) {
                    Some(DecodedValue::Uint64(val)) => (*val).try_into().ok(),
                    Some(DecodedValue::Uint256(val)) => (*val).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint256
        | EvmType::Uint160
        | EvmType::Uint128
        | EvmType::Uint96
        | EvmType::Uint80 => {
            let arr: StringArray = (0..num_rows)
                .map(|i| match get_tuple_field(i) {
                    Some(DecodedValue::Uint256(val)) => Some(val.to_string()),
                    Some(DecodedValue::Uint64(val)) => Some(val.to_string()),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int8 => {
            let arr: Int8Array = (0..num_rows)
                .map(|i| match get_tuple_field(i) {
                    Some(DecodedValue::Int8(val)) => Some(*val),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int64 => {
            let arr: Int64Array = (0..num_rows)
                .map(|i| match get_tuple_field(i) {
                    Some(DecodedValue::Int64(val)) => Some(*val),
                    Some(DecodedValue::Int256(val)) => (*val).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int32 | EvmType::Int24 => {
            let arr: Int32Array = (0..num_rows)
                .map(|i| match get_tuple_field(i) {
                    Some(DecodedValue::Int64(val)) => (*val).try_into().ok(),
                    Some(DecodedValue::Int256(val)) => (*val).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int16 => {
            let arr: Int16Array = (0..num_rows)
                .map(|i| match get_tuple_field(i) {
                    Some(DecodedValue::Int64(val)) => (*val).try_into().ok(),
                    Some(DecodedValue::Int256(val)) => (*val).try_into().ok(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int256 | EvmType::Int128 => {
            let arr: StringArray = (0..num_rows)
                .map(|i| match get_tuple_field(i) {
                    Some(DecodedValue::Int256(val)) => Some(val.to_string()),
                    Some(DecodedValue::Int64(val)) => Some(val.to_string()),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Bool => {
            let arr: BooleanArray = (0..num_rows)
                .map(|i| match get_tuple_field(i) {
                    Some(DecodedValue::Bool(val)) => Some(*val),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Bytes32 => {
            let mut builder = FixedSizeBinaryBuilder::with_capacity(num_rows, 32);
            for i in 0..num_rows {
                match get_tuple_field(i) {
                    Some(DecodedValue::Bytes32(b)) => builder.append_value(b)?,
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        EvmType::String => {
            let arr: StringArray = (0..num_rows)
                .map(|i| match get_tuple_field(i) {
                    Some(DecodedValue::String(s)) => Some(s.as_str()),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Bytes => {
            let arr: BinaryArray = (0..num_rows)
                .map(|i| match get_tuple_field(i) {
                    Some(DecodedValue::Bytes(b)) => Some(b.as_slice()),
                    Some(DecodedValue::Bytes32(b)) => Some(b.as_slice()),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Named { inner, .. } => build_once_tuple_field_array_aligned(
            address_arr,
            records_by_addr,
            function_name,
            field_idx,
            inner,
        ),
        EvmType::NamedTuple(_) => Err(EthCallDecodingError::Decode(
            "Nested NamedTuple not supported".to_string(),
        )),
        EvmType::UnnamedTuple(_) => Err(EthCallDecodingError::Decode(
            "Nested UnnamedTuple not supported".to_string(),
        )),
        EvmType::Array(_) => Err(EthCallDecodingError::Decode(
            "Nested Array in tuple fields not supported".to_string(),
        )),
    }
}

/// Build an Arrow array for a specific field of a named tuple from "once" calls
pub(super) fn build_once_tuple_field_array(
    records: &[DecodedOnceRecord],
    function_name: &str,
    field_idx: usize,
    field_type: &EvmType,
) -> Result<ArrayRef, EthCallDecodingError> {
    match field_type {
        EvmType::Address => {
            if records.is_empty() {
                Ok(Arc::new(FixedSizeBinaryBuilder::new(20).finish()))
            } else {
                let arr = FixedSizeBinaryArray::try_from_iter(
                    records
                        .iter()
                        .map(|r| extract_once_tuple_address(r, function_name, field_idx)),
                )?;
                Ok(Arc::new(arr))
            }
        }
        EvmType::Uint8 => {
            let arr: UInt8Array = records
                .iter()
                .map(|r| extract_once_tuple_uint8(r, function_name, field_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint64 => {
            let arr: UInt64Array = records
                .iter()
                .map(|r| extract_once_tuple_uint64(r, function_name, field_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint32 | EvmType::Uint24 => {
            let arr: UInt32Array = records
                .iter()
                .map(|r| extract_once_tuple_uint32(r, function_name, field_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint16 => {
            let arr: UInt16Array = records
                .iter()
                .map(|r| extract_once_tuple_uint16(r, function_name, field_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Uint256
        | EvmType::Uint160
        | EvmType::Uint128
        | EvmType::Uint96
        | EvmType::Uint80 => {
            let arr: StringArray = records
                .iter()
                .map(|r| extract_once_tuple_uint256_string(r, function_name, field_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int8 => {
            let arr: Int8Array = records
                .iter()
                .map(|r| extract_once_tuple_int8(r, function_name, field_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int64 => {
            let arr: Int64Array = records
                .iter()
                .map(|r| extract_once_tuple_int64(r, function_name, field_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int32 | EvmType::Int24 => {
            let arr: Int32Array = records
                .iter()
                .map(|r| extract_once_tuple_int32(r, function_name, field_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int16 => {
            let arr: Int16Array = records
                .iter()
                .map(|r| extract_once_tuple_int16(r, function_name, field_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Int256 | EvmType::Int128 => {
            let arr: StringArray = records
                .iter()
                .map(|r| extract_once_tuple_int256_string(r, function_name, field_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Bool => {
            let arr: BooleanArray = records
                .iter()
                .map(|r| extract_once_tuple_bool(r, function_name, field_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Bytes32 => {
            if records.is_empty() {
                Ok(Arc::new(FixedSizeBinaryBuilder::new(32).finish()))
            } else {
                let arr = FixedSizeBinaryArray::try_from_iter(
                    records
                        .iter()
                        .map(|r| extract_once_tuple_bytes32(r, function_name, field_idx)),
                )?;
                Ok(Arc::new(arr))
            }
        }
        EvmType::String => {
            let arr: StringArray = records
                .iter()
                .map(|r| extract_once_tuple_string(r, function_name, field_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Bytes => {
            let arr: BinaryArray = records
                .iter()
                .map(|r| extract_once_tuple_bytes(r, function_name, field_idx))
                .collect();
            Ok(Arc::new(arr))
        }
        EvmType::Named { inner, .. } => {
            build_once_tuple_field_array(records, function_name, field_idx, inner)
        }
        EvmType::NamedTuple(_) => Err(EthCallDecodingError::Decode(
            "Nested NamedTuple not supported".to_string(),
        )),
        EvmType::UnnamedTuple(_) => Err(EthCallDecodingError::Decode(
            "Nested UnnamedTuple not supported".to_string(),
        )),
        EvmType::Array(_) => Err(EthCallDecodingError::Decode(
            "Nested Array in tuple fields not supported".to_string(),
        )),
    }
}

// Helper functions for extracting tuple field values from "once" records

pub(super) fn extract_once_tuple_address<'a>(
    r: &'a DecodedOnceRecord,
    function_name: &str,
    idx: usize,
) -> &'a [u8] {
    match r.decoded_values.get(function_name) {
        Some(DecodedValue::NamedTuple(fields)) => {
            if let Some((_, DecodedValue::Address(addr))) = fields.get(idx) {
                addr.as_slice()
            } else {
                &[0u8; 20][..]
            }
        }
        _ => &[0u8; 20][..],
    }
}

pub(super) fn extract_once_tuple_uint8(
    r: &DecodedOnceRecord,
    function_name: &str,
    idx: usize,
) -> Option<u8> {
    match r.decoded_values.get(function_name) {
        Some(DecodedValue::NamedTuple(fields)) => fields.get(idx).and_then(|(_, v)| match v {
            DecodedValue::Uint8(val) => Some(*val),
            _ => None,
        }),
        _ => None,
    }
}

pub(super) fn extract_once_tuple_uint64(
    r: &DecodedOnceRecord,
    function_name: &str,
    idx: usize,
) -> Option<u64> {
    match r.decoded_values.get(function_name) {
        Some(DecodedValue::NamedTuple(fields)) => fields.get(idx).and_then(|(_, v)| match v {
            DecodedValue::Uint64(val) => Some(*val),
            DecodedValue::Uint256(val) => (*val).try_into().ok(),
            _ => None,
        }),
        _ => None,
    }
}

pub(super) fn extract_once_tuple_uint32(
    r: &DecodedOnceRecord,
    function_name: &str,
    idx: usize,
) -> Option<u32> {
    match r.decoded_values.get(function_name) {
        Some(DecodedValue::NamedTuple(fields)) => fields.get(idx).and_then(|(_, v)| match v {
            DecodedValue::Uint64(val) => (*val).try_into().ok(),
            DecodedValue::Uint256(val) => (*val).try_into().ok(),
            _ => None,
        }),
        _ => None,
    }
}

pub(super) fn extract_once_tuple_uint16(
    r: &DecodedOnceRecord,
    function_name: &str,
    idx: usize,
) -> Option<u16> {
    match r.decoded_values.get(function_name) {
        Some(DecodedValue::NamedTuple(fields)) => fields.get(idx).and_then(|(_, v)| match v {
            DecodedValue::Uint64(val) => (*val).try_into().ok(),
            DecodedValue::Uint256(val) => (*val).try_into().ok(),
            _ => None,
        }),
        _ => None,
    }
}

pub(super) fn extract_once_tuple_uint256_string(
    r: &DecodedOnceRecord,
    function_name: &str,
    idx: usize,
) -> Option<String> {
    match r.decoded_values.get(function_name) {
        Some(DecodedValue::NamedTuple(fields)) => fields.get(idx).and_then(|(_, v)| match v {
            DecodedValue::Uint256(val) => Some(val.to_string()),
            DecodedValue::Uint64(val) => Some(val.to_string()),
            _ => None,
        }),
        _ => None,
    }
}

pub(super) fn extract_once_tuple_int8(
    r: &DecodedOnceRecord,
    function_name: &str,
    idx: usize,
) -> Option<i8> {
    match r.decoded_values.get(function_name) {
        Some(DecodedValue::NamedTuple(fields)) => fields.get(idx).and_then(|(_, v)| match v {
            DecodedValue::Int8(val) => Some(*val),
            _ => None,
        }),
        _ => None,
    }
}

pub(super) fn extract_once_tuple_int64(
    r: &DecodedOnceRecord,
    function_name: &str,
    idx: usize,
) -> Option<i64> {
    match r.decoded_values.get(function_name) {
        Some(DecodedValue::NamedTuple(fields)) => fields.get(idx).and_then(|(_, v)| match v {
            DecodedValue::Int64(val) => Some(*val),
            DecodedValue::Int256(val) => (*val).try_into().ok(),
            _ => None,
        }),
        _ => None,
    }
}

pub(super) fn extract_once_tuple_int32(
    r: &DecodedOnceRecord,
    function_name: &str,
    idx: usize,
) -> Option<i32> {
    match r.decoded_values.get(function_name) {
        Some(DecodedValue::NamedTuple(fields)) => fields.get(idx).and_then(|(_, v)| match v {
            DecodedValue::Int64(val) => (*val).try_into().ok(),
            DecodedValue::Int256(val) => (*val).try_into().ok(),
            _ => None,
        }),
        _ => None,
    }
}

pub(super) fn extract_once_tuple_int16(
    r: &DecodedOnceRecord,
    function_name: &str,
    idx: usize,
) -> Option<i16> {
    match r.decoded_values.get(function_name) {
        Some(DecodedValue::NamedTuple(fields)) => fields.get(idx).and_then(|(_, v)| match v {
            DecodedValue::Int64(val) => (*val).try_into().ok(),
            DecodedValue::Int256(val) => (*val).try_into().ok(),
            _ => None,
        }),
        _ => None,
    }
}

pub(super) fn extract_once_tuple_int256_string(
    r: &DecodedOnceRecord,
    function_name: &str,
    idx: usize,
) -> Option<String> {
    match r.decoded_values.get(function_name) {
        Some(DecodedValue::NamedTuple(fields)) => fields.get(idx).and_then(|(_, v)| match v {
            DecodedValue::Int256(val) => Some(val.to_string()),
            DecodedValue::Int64(val) => Some(val.to_string()),
            _ => None,
        }),
        _ => None,
    }
}

pub(super) fn extract_once_tuple_bool(
    r: &DecodedOnceRecord,
    function_name: &str,
    idx: usize,
) -> Option<bool> {
    match r.decoded_values.get(function_name) {
        Some(DecodedValue::NamedTuple(fields)) => fields.get(idx).and_then(|(_, v)| match v {
            DecodedValue::Bool(val) => Some(*val),
            _ => None,
        }),
        _ => None,
    }
}

pub(super) fn extract_once_tuple_bytes32<'a>(
    r: &'a DecodedOnceRecord,
    function_name: &str,
    idx: usize,
) -> &'a [u8] {
    match r.decoded_values.get(function_name) {
        Some(DecodedValue::NamedTuple(fields)) => {
            if let Some((_, DecodedValue::Bytes32(b))) = fields.get(idx) {
                b.as_slice()
            } else {
                &[0u8; 32][..]
            }
        }
        _ => &[0u8; 32][..],
    }
}

pub(super) fn extract_once_tuple_string<'a>(
    r: &'a DecodedOnceRecord,
    function_name: &str,
    idx: usize,
) -> Option<&'a str> {
    match r.decoded_values.get(function_name) {
        Some(DecodedValue::NamedTuple(fields)) => fields.get(idx).and_then(|(_, v)| match v {
            DecodedValue::String(s) => Some(s.as_str()),
            _ => None,
        }),
        _ => None,
    }
}

pub(super) fn extract_once_tuple_bytes<'a>(
    r: &'a DecodedOnceRecord,
    function_name: &str,
    idx: usize,
) -> Option<&'a [u8]> {
    match r.decoded_values.get(function_name) {
        Some(DecodedValue::NamedTuple(fields)) => fields.get(idx).and_then(|(_, v)| match v {
            DecodedValue::Bytes(b) => Some(b.as_slice()),
            DecodedValue::Bytes32(b) => Some(b.as_slice()),
            _ => None,
        }),
        _ => None,
    }
}
