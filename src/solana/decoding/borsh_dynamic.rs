//! Runtime Borsh deserializer driven by IDL type definitions.
//!
//! Walks `IdlType` trees and produces `DecodedValue` without depending on
//! the `borsh` crate -- all byte reading is done manually from a cursor.

use std::collections::HashMap;

use crate::types::chain::ChainAddress;
use crate::types::decoded::DecodedValue;

use super::idl::{IdlType, IdlTypeDef};
use super::traits::SolanaDecodeError;

// ---------------------------------------------------------------------------
// Cursor helpers
// ---------------------------------------------------------------------------

fn read_u8(cursor: &mut &[u8]) -> Result<u8, SolanaDecodeError> {
    if cursor.is_empty() {
        return Err(SolanaDecodeError::UnexpectedEof {
            needed: 1,
            available: 0,
        });
    }
    let val = cursor[0];
    *cursor = &cursor[1..];
    Ok(val)
}

fn read_bytes<'a>(cursor: &mut &'a [u8], len: usize) -> Result<&'a [u8], SolanaDecodeError> {
    if cursor.len() < len {
        return Err(SolanaDecodeError::UnexpectedEof {
            needed: len,
            available: cursor.len(),
        });
    }
    let (head, tail) = cursor.split_at(len);
    *cursor = tail;
    Ok(head)
}

fn read_le<const N: usize>(cursor: &mut &[u8]) -> Result<[u8; N], SolanaDecodeError> {
    let bytes = read_bytes(cursor, N)?;
    let mut arr = [0u8; N];
    arr.copy_from_slice(bytes);
    Ok(arr)
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Deserialize a single value from a Borsh byte stream according to `idl_type`.
pub fn deserialize_value(
    cursor: &mut &[u8],
    idl_type: &IdlType,
    defined_types: &HashMap<String, IdlTypeDef>,
) -> Result<DecodedValue, SolanaDecodeError> {
    match idl_type {
        IdlType::Bool => {
            let byte = read_u8(cursor)?;
            Ok(DecodedValue::Bool(byte != 0))
        }

        IdlType::U8 => {
            let val = read_u8(cursor)?;
            Ok(DecodedValue::Uint8(val))
        }
        IdlType::U16 => {
            let bytes = read_le::<2>(cursor)?;
            Ok(DecodedValue::Uint32(u16::from_le_bytes(bytes) as u32))
        }
        IdlType::U32 => {
            let bytes = read_le::<4>(cursor)?;
            Ok(DecodedValue::Uint32(u32::from_le_bytes(bytes)))
        }
        IdlType::U64 => {
            let bytes = read_le::<8>(cursor)?;
            Ok(DecodedValue::Uint64(u64::from_le_bytes(bytes)))
        }
        IdlType::U128 => {
            let bytes = read_le::<16>(cursor)?;
            Ok(DecodedValue::Uint128(u128::from_le_bytes(bytes)))
        }

        IdlType::I8 => {
            let byte = read_u8(cursor)?;
            Ok(DecodedValue::Int8(byte as i8))
        }
        IdlType::I16 => {
            let bytes = read_le::<2>(cursor)?;
            Ok(DecodedValue::Int32(i16::from_le_bytes(bytes) as i32))
        }
        IdlType::I32 => {
            let bytes = read_le::<4>(cursor)?;
            Ok(DecodedValue::Int32(i32::from_le_bytes(bytes)))
        }
        IdlType::I64 => {
            let bytes = read_le::<8>(cursor)?;
            Ok(DecodedValue::Int64(i64::from_le_bytes(bytes)))
        }
        IdlType::I128 => {
            let bytes = read_le::<16>(cursor)?;
            Ok(DecodedValue::Int128(i128::from_le_bytes(bytes)))
        }

        IdlType::F32 => {
            let bytes = read_le::<4>(cursor)?;
            Ok(DecodedValue::Float(f32::from_le_bytes(bytes) as f64))
        }
        IdlType::F64 => {
            let bytes = read_le::<8>(cursor)?;
            Ok(DecodedValue::Float(f64::from_le_bytes(bytes)))
        }

        IdlType::Pubkey => {
            let bytes = read_le::<32>(cursor)?;
            Ok(DecodedValue::ChainAddress(ChainAddress::Solana(bytes)))
        }

        IdlType::String => {
            let len_bytes = read_le::<4>(cursor)?;
            let len = u32::from_le_bytes(len_bytes) as usize;
            let bytes = read_bytes(cursor, len)?;
            let s = std::str::from_utf8(bytes).map_err(|e| {
                SolanaDecodeError::IdlParse(format!("invalid UTF-8 in Borsh string: {}", e))
            })?;
            Ok(DecodedValue::String(s.to_owned()))
        }

        IdlType::Bytes => {
            let len_bytes = read_le::<4>(cursor)?;
            let len = u32::from_le_bytes(len_bytes) as usize;
            let bytes = read_bytes(cursor, len)?;
            Ok(DecodedValue::Bytes(bytes.to_vec()))
        }

        IdlType::Option(inner) => {
            let tag = read_u8(cursor)?;
            if tag == 0 {
                Ok(DecodedValue::Null)
            } else {
                deserialize_value(cursor, inner, defined_types)
            }
        }

        IdlType::Vec(inner) => {
            let len_bytes = read_le::<4>(cursor)?;
            let len = u32::from_le_bytes(len_bytes) as usize;
            // Cap pre-allocation to remaining bytes to prevent OOM on malformed input.
            let safe_cap = len.min(cursor.len());
            let mut items = Vec::with_capacity(safe_cap);
            for _ in 0..len {
                items.push(deserialize_value(cursor, inner, defined_types)?);
            }
            Ok(DecodedValue::Array(items))
        }

        IdlType::Array(inner, size) => {
            let mut items = Vec::with_capacity(*size);
            for _ in 0..*size {
                items.push(deserialize_value(cursor, inner, defined_types)?);
            }
            Ok(DecodedValue::Array(items))
        }

        IdlType::Defined(name) => {
            let type_def = defined_types.get(name).ok_or_else(|| {
                SolanaDecodeError::UnknownType(name.clone())
            })?;

            match type_def {
                IdlTypeDef::Struct { fields } => {
                    let mut pairs = Vec::with_capacity(fields.len());
                    for (field_name, field_type) in fields {
                        let val = deserialize_value(cursor, field_type, defined_types)?;
                        pairs.push((field_name.clone(), val));
                    }
                    Ok(DecodedValue::NamedTuple(pairs))
                }

                IdlTypeDef::Enum { variants } => {
                    let variant_idx = read_u8(cursor)? as usize;
                    if variant_idx >= variants.len() {
                        return Err(SolanaDecodeError::InvalidEnumVariant(variant_idx));
                    }
                    let variant = &variants[variant_idx];

                    match &variant.fields {
                        None => Ok(DecodedValue::String(variant.name.clone())),
                        Some(fields) => {
                            let mut pairs = Vec::with_capacity(fields.len() + 1);
                            pairs.push((
                                "variant".to_owned(),
                                DecodedValue::String(variant.name.clone()),
                            ));
                            for (field_name, field_type) in fields {
                                let val =
                                    deserialize_value(cursor, field_type, defined_types)?;
                                pairs.push((field_name.clone(), val));
                            }
                            Ok(DecodedValue::NamedTuple(pairs))
                        }
                    }
                }
            }
        }
    }
}

/// Deserialize a sequence of named struct fields into a `HashMap`.
///
/// This is the primary entry point used by `AnchorDecoder` when decoding
/// events, instruction args, and account state.
pub fn deserialize_struct_fields(
    cursor: &mut &[u8],
    fields: &[(String, IdlType)],
    defined_types: &HashMap<String, IdlTypeDef>,
) -> Result<HashMap<String, DecodedValue>, SolanaDecodeError> {
    let mut map = HashMap::with_capacity(fields.len());
    for (name, idl_type) in fields {
        let val = deserialize_value(cursor, idl_type, defined_types)?;
        map.insert(name.clone(), val);
    }
    Ok(map)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Bool
    // -----------------------------------------------------------------------

    #[test]
    fn test_bool_true() {
        let data: &[u8] = &[1];
        let mut cursor: &[u8] = data;
        let val = deserialize_value(&mut cursor, &IdlType::Bool, &HashMap::new()).unwrap();
        assert_eq!(val.as_bool(), Some(true));
        assert!(cursor.is_empty());
    }

    #[test]
    fn test_bool_false() {
        let data: &[u8] = &[0];
        let mut cursor: &[u8] = data;
        let val = deserialize_value(&mut cursor, &IdlType::Bool, &HashMap::new()).unwrap();
        assert_eq!(val.as_bool(), Some(false));
    }

    // -----------------------------------------------------------------------
    // Unsigned integers
    // -----------------------------------------------------------------------

    #[test]
    fn test_u8() {
        let data: &[u8] = &[42];
        let mut cursor: &[u8] = data;
        let val = deserialize_value(&mut cursor, &IdlType::U8, &HashMap::new()).unwrap();
        assert_eq!(val.as_u8(), Some(42));
    }

    #[test]
    fn test_u16_upcasts_to_u32() {
        let data = 1000u16.to_le_bytes();
        let mut cursor: &[u8] = &data;
        let val = deserialize_value(&mut cursor, &IdlType::U16, &HashMap::new()).unwrap();
        // U16 produces Uint32
        assert_eq!(val.as_u32(), Some(1000));
        assert!(cursor.is_empty());
    }

    #[test]
    fn test_u32() {
        let data = 70000u32.to_le_bytes();
        let mut cursor: &[u8] = &data;
        let val = deserialize_value(&mut cursor, &IdlType::U32, &HashMap::new()).unwrap();
        assert_eq!(val.as_u32(), Some(70000));
    }

    #[test]
    fn test_u64() {
        let data = 123456789u64.to_le_bytes();
        let mut cursor: &[u8] = &data;
        let val = deserialize_value(&mut cursor, &IdlType::U64, &HashMap::new()).unwrap();
        assert_eq!(val.as_u64(), Some(123456789));
    }

    #[test]
    fn test_u128() {
        let big: u128 = u128::MAX - 1;
        let data = big.to_le_bytes();
        let mut cursor: &[u8] = &data;
        let val = deserialize_value(&mut cursor, &IdlType::U128, &HashMap::new()).unwrap();
        match &val {
            DecodedValue::Uint128(v) => assert_eq!(*v, big),
            other => panic!("expected Uint128, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // Signed integers
    // -----------------------------------------------------------------------

    #[test]
    fn test_i8() {
        let data: &[u8] = &[(-5i8) as u8];
        let mut cursor: &[u8] = data;
        let val = deserialize_value(&mut cursor, &IdlType::I8, &HashMap::new()).unwrap();
        assert_eq!(val.as_i64(), Some(-5));
    }

    #[test]
    fn test_i16_upcasts_to_i32() {
        let data = (-300i16).to_le_bytes();
        let mut cursor: &[u8] = &data;
        let val = deserialize_value(&mut cursor, &IdlType::I16, &HashMap::new()).unwrap();
        assert_eq!(val.as_i32(), Some(-300));
        assert!(cursor.is_empty());
    }

    #[test]
    fn test_i32() {
        let data = (-100_000i32).to_le_bytes();
        let mut cursor: &[u8] = &data;
        let val = deserialize_value(&mut cursor, &IdlType::I32, &HashMap::new()).unwrap();
        assert_eq!(val.as_i32(), Some(-100_000));
    }

    #[test]
    fn test_i64() {
        let data = (-999_999_999i64).to_le_bytes();
        let mut cursor: &[u8] = &data;
        let val = deserialize_value(&mut cursor, &IdlType::I64, &HashMap::new()).unwrap();
        assert_eq!(val.as_i64(), Some(-999_999_999));
    }

    #[test]
    fn test_i128() {
        let big: i128 = i128::MIN + 1;
        let data = big.to_le_bytes();
        let mut cursor: &[u8] = &data;
        let val = deserialize_value(&mut cursor, &IdlType::I128, &HashMap::new()).unwrap();
        match &val {
            DecodedValue::Int128(v) => assert_eq!(*v, big),
            other => panic!("expected Int128, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // Floats
    // -----------------------------------------------------------------------

    #[test]
    fn test_f32() {
        let data = 3.14f32.to_le_bytes();
        let mut cursor: &[u8] = &data;
        let val = deserialize_value(&mut cursor, &IdlType::F32, &HashMap::new()).unwrap();
        let f = val.as_float().unwrap();
        assert!((f - 3.14f32 as f64).abs() < 1e-5);
    }

    #[test]
    fn test_f64() {
        let data = std::f64::consts::PI.to_le_bytes();
        let mut cursor: &[u8] = &data;
        let val = deserialize_value(&mut cursor, &IdlType::F64, &HashMap::new()).unwrap();
        let f = val.as_float().unwrap();
        assert!((f - std::f64::consts::PI).abs() < 1e-15);
    }

    // -----------------------------------------------------------------------
    // Pubkey
    // -----------------------------------------------------------------------

    #[test]
    fn test_pubkey() {
        let mut key = [0u8; 32];
        key[0] = 0xAB;
        key[31] = 0xCD;
        let mut cursor: &[u8] = &key;
        let val = deserialize_value(&mut cursor, &IdlType::Pubkey, &HashMap::new()).unwrap();
        match &val {
            DecodedValue::ChainAddress(ChainAddress::Solana(bytes)) => {
                assert_eq!(bytes[0], 0xAB);
                assert_eq!(bytes[31], 0xCD);
            }
            other => panic!("expected ChainAddress::Solana, got {:?}", other),
        }
        assert!(cursor.is_empty());
    }

    // -----------------------------------------------------------------------
    // String
    // -----------------------------------------------------------------------

    #[test]
    fn test_string() {
        let s = "hello world";
        let len = (s.len() as u32).to_le_bytes();
        let mut data = Vec::new();
        data.extend_from_slice(&len);
        data.extend_from_slice(s.as_bytes());

        let mut cursor: &[u8] = &data;
        let val = deserialize_value(&mut cursor, &IdlType::String, &HashMap::new()).unwrap();
        assert_eq!(val.as_string(), Some("hello world"));
        assert!(cursor.is_empty());
    }

    #[test]
    fn test_string_empty() {
        let data = 0u32.to_le_bytes();
        let mut cursor: &[u8] = &data;
        let val = deserialize_value(&mut cursor, &IdlType::String, &HashMap::new()).unwrap();
        assert_eq!(val.as_string(), Some(""));
    }

    // -----------------------------------------------------------------------
    // Bytes
    // -----------------------------------------------------------------------

    #[test]
    fn test_bytes() {
        let payload = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let len = (payload.len() as u32).to_le_bytes();
        let mut data = Vec::new();
        data.extend_from_slice(&len);
        data.extend_from_slice(&payload);

        let mut cursor: &[u8] = &data;
        let val = deserialize_value(&mut cursor, &IdlType::Bytes, &HashMap::new()).unwrap();
        match &val {
            DecodedValue::Bytes(b) => assert_eq!(b, &payload),
            other => panic!("expected Bytes, got {:?}", other),
        }
    }

    #[test]
    fn test_bytes_empty() {
        let data = 0u32.to_le_bytes();
        let mut cursor: &[u8] = &data;
        let val = deserialize_value(&mut cursor, &IdlType::Bytes, &HashMap::new()).unwrap();
        match &val {
            DecodedValue::Bytes(b) => assert!(b.is_empty()),
            other => panic!("expected Bytes, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // Option
    // -----------------------------------------------------------------------

    #[test]
    fn test_option_some() {
        let mut data = vec![1u8]; // tag = Some
        data.extend_from_slice(&42u32.to_le_bytes());

        let mut cursor: &[u8] = &data;
        let val = deserialize_value(
            &mut cursor,
            &IdlType::Option(Box::new(IdlType::U32)),
            &HashMap::new(),
        )
        .unwrap();
        assert_eq!(val.as_u32(), Some(42));
    }

    #[test]
    fn test_option_none() {
        let data = vec![0u8]; // tag = None
        let mut cursor: &[u8] = &data;
        let val = deserialize_value(
            &mut cursor,
            &IdlType::Option(Box::new(IdlType::U32)),
            &HashMap::new(),
        )
        .unwrap();
        assert!(val.is_null());
    }

    // -----------------------------------------------------------------------
    // Vec
    // -----------------------------------------------------------------------

    #[test]
    fn test_vec() {
        let mut data = Vec::new();
        data.extend_from_slice(&3u32.to_le_bytes()); // len = 3
        data.push(10);
        data.push(20);
        data.push(30);

        let mut cursor: &[u8] = &data;
        let val = deserialize_value(
            &mut cursor,
            &IdlType::Vec(Box::new(IdlType::U8)),
            &HashMap::new(),
        )
        .unwrap();

        match &val {
            DecodedValue::Array(items) => {
                assert_eq!(items.len(), 3);
                assert_eq!(items[0].as_u8(), Some(10));
                assert_eq!(items[1].as_u8(), Some(20));
                assert_eq!(items[2].as_u8(), Some(30));
            }
            other => panic!("expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_vec_empty() {
        let data = 0u32.to_le_bytes();
        let mut cursor: &[u8] = &data;
        let val = deserialize_value(
            &mut cursor,
            &IdlType::Vec(Box::new(IdlType::U64)),
            &HashMap::new(),
        )
        .unwrap();
        match &val {
            DecodedValue::Array(items) => assert!(items.is_empty()),
            other => panic!("expected Array, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // Fixed-size array
    // -----------------------------------------------------------------------

    #[test]
    fn test_fixed_array() {
        let mut data = Vec::new();
        data.extend_from_slice(&100u16.to_le_bytes());
        data.extend_from_slice(&200u16.to_le_bytes());

        let mut cursor: &[u8] = &data;
        let val = deserialize_value(
            &mut cursor,
            &IdlType::Array(Box::new(IdlType::U16), 2),
            &HashMap::new(),
        )
        .unwrap();

        match &val {
            DecodedValue::Array(items) => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0].as_u32(), Some(100));
                assert_eq!(items[1].as_u32(), Some(200));
            }
            other => panic!("expected Array, got {:?}", other),
        }
        assert!(cursor.is_empty());
    }

    // -----------------------------------------------------------------------
    // Struct (Defined)
    // -----------------------------------------------------------------------

    #[test]
    fn test_struct() {
        let mut defined_types = HashMap::new();
        defined_types.insert(
            "Point".to_string(),
            IdlTypeDef::Struct {
                fields: vec![
                    ("x".to_string(), IdlType::I32),
                    ("y".to_string(), IdlType::I32),
                ],
            },
        );

        let mut data = Vec::new();
        data.extend_from_slice(&10i32.to_le_bytes());
        data.extend_from_slice(&(-20i32).to_le_bytes());

        let mut cursor: &[u8] = &data;
        let val =
            deserialize_value(&mut cursor, &IdlType::Defined("Point".to_string()), &defined_types)
                .unwrap();

        match &val {
            DecodedValue::NamedTuple(fields) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].0, "x");
                assert_eq!(fields[0].1.as_i32(), Some(10));
                assert_eq!(fields[1].0, "y");
                assert_eq!(fields[1].1.as_i32(), Some(-20));
            }
            other => panic!("expected NamedTuple, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // Enum — unit variant
    // -----------------------------------------------------------------------

    #[test]
    fn test_enum_unit() {
        let mut defined_types = HashMap::new();
        defined_types.insert(
            "Color".to_string(),
            IdlTypeDef::Enum {
                variants: vec![
                    super::super::idl::IdlEnumVariant {
                        name: "Red".to_string(),
                        fields: None,
                    },
                    super::super::idl::IdlEnumVariant {
                        name: "Green".to_string(),
                        fields: None,
                    },
                    super::super::idl::IdlEnumVariant {
                        name: "Blue".to_string(),
                        fields: None,
                    },
                ],
            },
        );

        // variant index = 1 => "Green"
        let data = [1u8];
        let mut cursor: &[u8] = &data;
        let val =
            deserialize_value(&mut cursor, &IdlType::Defined("Color".to_string()), &defined_types)
                .unwrap();

        assert_eq!(val.as_string(), Some("Green"));
    }

    // -----------------------------------------------------------------------
    // Enum — variant with fields
    // -----------------------------------------------------------------------

    #[test]
    fn test_enum_with_fields() {
        let mut defined_types = HashMap::new();
        defined_types.insert(
            "Shape".to_string(),
            IdlTypeDef::Enum {
                variants: vec![
                    super::super::idl::IdlEnumVariant {
                        name: "Circle".to_string(),
                        fields: Some(vec![("radius".to_string(), IdlType::U32)]),
                    },
                    super::super::idl::IdlEnumVariant {
                        name: "Rect".to_string(),
                        fields: Some(vec![
                            ("w".to_string(), IdlType::U32),
                            ("h".to_string(), IdlType::U32),
                        ]),
                    },
                ],
            },
        );

        let mut data = Vec::new();
        data.push(1); // variant index 1 => Rect
        data.extend_from_slice(&5u32.to_le_bytes()); // w = 5
        data.extend_from_slice(&10u32.to_le_bytes()); // h = 10

        let mut cursor: &[u8] = &data;
        let val =
            deserialize_value(&mut cursor, &IdlType::Defined("Shape".to_string()), &defined_types)
                .unwrap();

        match &val {
            DecodedValue::NamedTuple(fields) => {
                assert_eq!(fields[0].0, "variant");
                assert_eq!(fields[0].1.as_string(), Some("Rect"));
                assert_eq!(fields[1].0, "w");
                assert_eq!(fields[1].1.as_u32(), Some(5));
                assert_eq!(fields[2].0, "h");
                assert_eq!(fields[2].1.as_u32(), Some(10));
            }
            other => panic!("expected NamedTuple, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // Nested struct
    // -----------------------------------------------------------------------

    #[test]
    fn test_nested_struct() {
        let mut defined_types = HashMap::new();
        defined_types.insert(
            "Inner".to_string(),
            IdlTypeDef::Struct {
                fields: vec![("val".to_string(), IdlType::U64)],
            },
        );
        defined_types.insert(
            "Outer".to_string(),
            IdlTypeDef::Struct {
                fields: vec![
                    ("label".to_string(), IdlType::U8),
                    ("inner".to_string(), IdlType::Defined("Inner".to_string())),
                ],
            },
        );

        let mut data = Vec::new();
        data.push(7u8); // label = 7
        data.extend_from_slice(&42u64.to_le_bytes()); // inner.val = 42

        let mut cursor: &[u8] = &data;
        let val =
            deserialize_value(&mut cursor, &IdlType::Defined("Outer".to_string()), &defined_types)
                .unwrap();

        match &val {
            DecodedValue::NamedTuple(fields) => {
                assert_eq!(fields[0].0, "label");
                assert_eq!(fields[0].1.as_u8(), Some(7));
                assert_eq!(fields[1].0, "inner");
                match &fields[1].1 {
                    DecodedValue::NamedTuple(inner_fields) => {
                        assert_eq!(inner_fields[0].0, "val");
                        assert_eq!(inner_fields[0].1.as_u64(), Some(42));
                    }
                    other => panic!("expected NamedTuple for inner, got {:?}", other),
                }
            }
            other => panic!("expected NamedTuple, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // Error cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_unexpected_eof() {
        let data: &[u8] = &[0x01]; // only 1 byte, but u32 needs 4
        let mut cursor: &[u8] = data;
        let result = deserialize_value(&mut cursor, &IdlType::U32, &HashMap::new());
        match result {
            Err(SolanaDecodeError::UnexpectedEof {
                needed: 4,
                available: 1,
            }) => {}
            other => panic!("expected UnexpectedEof, got {:?}", other),
        }
    }

    #[test]
    fn test_unknown_type() {
        let data: &[u8] = &[];
        let mut cursor: &[u8] = data;
        let result = deserialize_value(
            &mut cursor,
            &IdlType::Defined("Nonexistent".to_string()),
            &HashMap::new(),
        );
        match result {
            Err(SolanaDecodeError::UnknownType(name)) => {
                assert_eq!(name, "Nonexistent");
            }
            other => panic!("expected UnknownType, got {:?}", other),
        }
    }

    #[test]
    fn test_invalid_enum_variant() {
        let mut defined_types = HashMap::new();
        defined_types.insert(
            "Small".to_string(),
            IdlTypeDef::Enum {
                variants: vec![super::super::idl::IdlEnumVariant {
                    name: "Only".to_string(),
                    fields: None,
                }],
            },
        );

        // variant index 5 is out of range (only 1 variant exists)
        let data = [5u8];
        let mut cursor: &[u8] = &data;
        let result =
            deserialize_value(&mut cursor, &IdlType::Defined("Small".to_string()), &defined_types);
        match result {
            Err(SolanaDecodeError::InvalidEnumVariant(5)) => {}
            other => panic!("expected InvalidEnumVariant(5), got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // deserialize_struct_fields
    // -----------------------------------------------------------------------

    #[test]
    fn test_deserialize_struct_fields() {
        let fields = vec![
            ("enabled".to_string(), IdlType::Bool),
            ("count".to_string(), IdlType::U64),
            ("name".to_string(), IdlType::String),
        ];

        let mut data = Vec::new();
        data.push(1u8); // enabled = true
        data.extend_from_slice(&42u64.to_le_bytes()); // count = 42
        let name = "test";
        data.extend_from_slice(&(name.len() as u32).to_le_bytes());
        data.extend_from_slice(name.as_bytes());

        let mut cursor: &[u8] = &data;
        let map = deserialize_struct_fields(&mut cursor, &fields, &HashMap::new()).unwrap();

        assert_eq!(map.len(), 3);
        assert_eq!(map["enabled"].as_bool(), Some(true));
        assert_eq!(map["count"].as_u64(), Some(42));
        assert_eq!(map["name"].as_string(), Some("test"));
        assert!(cursor.is_empty());
    }

    // -----------------------------------------------------------------------
    // Complex struct with mixed types
    // -----------------------------------------------------------------------

    #[test]
    fn test_complex_struct() {
        let mut defined_types = HashMap::new();
        defined_types.insert(
            "Complex".to_string(),
            IdlTypeDef::Struct {
                fields: vec![
                    ("amount".to_string(), IdlType::U64),
                    ("authority".to_string(), IdlType::Pubkey),
                    ("maybe_fee".to_string(), IdlType::Option(Box::new(IdlType::U32))),
                    ("tags".to_string(), IdlType::Vec(Box::new(IdlType::U8))),
                ],
            },
        );

        let mut data = Vec::new();

        // amount = 1_000_000
        data.extend_from_slice(&1_000_000u64.to_le_bytes());

        // authority = [0xAA; 32]
        data.extend_from_slice(&[0xAA; 32]);

        // maybe_fee = Some(500)
        data.push(1); // tag = Some
        data.extend_from_slice(&500u32.to_le_bytes());

        // tags = [1, 2, 3]
        data.extend_from_slice(&3u32.to_le_bytes()); // len = 3
        data.push(1);
        data.push(2);
        data.push(3);

        let mut cursor: &[u8] = &data;
        let val = deserialize_value(
            &mut cursor,
            &IdlType::Defined("Complex".to_string()),
            &defined_types,
        )
        .unwrap();

        match &val {
            DecodedValue::NamedTuple(fields) => {
                assert_eq!(fields.len(), 4);

                // amount
                assert_eq!(fields[0].0, "amount");
                assert_eq!(fields[0].1.as_u64(), Some(1_000_000));

                // authority
                assert_eq!(fields[1].0, "authority");
                assert_eq!(
                    fields[1].1.as_pubkey(),
                    Some([0xAA; 32])
                );

                // maybe_fee
                assert_eq!(fields[2].0, "maybe_fee");
                assert_eq!(fields[2].1.as_u32(), Some(500));

                // tags
                assert_eq!(fields[3].0, "tags");
                match &fields[3].1 {
                    DecodedValue::Array(items) => {
                        assert_eq!(items.len(), 3);
                        assert_eq!(items[0].as_u8(), Some(1));
                        assert_eq!(items[1].as_u8(), Some(2));
                        assert_eq!(items[2].as_u8(), Some(3));
                    }
                    other => panic!("expected Array for tags, got {:?}", other),
                }
            }
            other => panic!("expected NamedTuple, got {:?}", other),
        }

        assert!(cursor.is_empty());
    }

    #[test]
    fn test_vec_malformed_length_does_not_oom() {
        // 4-byte length claiming 1 billion elements, but only 2 bytes of actual data.
        let mut data = Vec::new();
        data.extend_from_slice(&1_000_000_000u32.to_le_bytes());
        data.extend_from_slice(&[0x01, 0x02]);

        let mut cursor: &[u8] = &data;
        let result = deserialize_value(&mut cursor, &IdlType::Vec(Box::new(IdlType::U8)), &HashMap::new());
        // Must fail with UnexpectedEof, not OOM.
        assert!(matches!(result, Err(SolanaDecodeError::UnexpectedEof { .. })));
    }
}
