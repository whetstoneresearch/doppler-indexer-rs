//! Value decoding: alloy DynSolType -> DecodedValue.

use alloy::dyn_abi::{DynSolType, DynSolValue};

use super::types::EthCallDecodingError;
use crate::types::config::eth_call::EvmType;
use crate::types::decoded::DecodedValue;

fn evm_type_to_dyn_sol_type(output_type: &EvmType) -> DynSolType {
    match output_type {
        EvmType::Int256 => DynSolType::Int(256),
        EvmType::Int128 => DynSolType::Int(128),
        EvmType::Int64 => DynSolType::Int(64),
        EvmType::Int32 => DynSolType::Int(32),
        EvmType::Int24 => DynSolType::Int(24),
        EvmType::Int16 => DynSolType::Int(16),
        EvmType::Int8 => DynSolType::Int(8),
        EvmType::Uint256 => DynSolType::Uint(256),
        EvmType::Uint160 => DynSolType::Uint(160),
        EvmType::Uint128 => DynSolType::Uint(128),
        EvmType::Uint96 => DynSolType::Uint(96),
        EvmType::Uint80 => DynSolType::Uint(80),
        EvmType::Uint64 => DynSolType::Uint(64),
        EvmType::Uint32 => DynSolType::Uint(32),
        EvmType::Uint24 => DynSolType::Uint(24),
        EvmType::Uint16 => DynSolType::Uint(16),
        EvmType::Uint8 => DynSolType::Uint(8),
        EvmType::Address => DynSolType::Address,
        EvmType::Bool => DynSolType::Bool,
        EvmType::Bytes32 => DynSolType::FixedBytes(32),
        EvmType::Bytes => DynSolType::Bytes,
        EvmType::String => DynSolType::String,
        EvmType::Named { inner, .. } => evm_type_to_dyn_sol_type(inner),
        EvmType::NamedTuple(fields) => {
            let field_types: Vec<DynSolType> = fields
                .iter()
                .map(|(_, ty)| evm_type_to_dyn_sol_type(ty))
                .collect();
            DynSolType::Tuple(field_types)
        }
        EvmType::UnnamedTuple(fields) => {
            let field_types: Vec<DynSolType> =
                fields.iter().map(evm_type_to_dyn_sol_type).collect();
            DynSolType::Tuple(field_types)
        }
        EvmType::Array(inner) => DynSolType::Array(Box::new(evm_type_to_dyn_sol_type(inner))),
    }
}

/// Decode a raw value using the specified type
pub fn decode_value(
    raw: &[u8],
    output_type: &EvmType,
) -> Result<DecodedValue, EthCallDecodingError> {
    let sol_type = evm_type_to_dyn_sol_type(output_type);

    let decoded = sol_type
        .abi_decode_params(raw)
        .map_err(|e| EthCallDecodingError::Decode(e.to_string()))?;

    convert_dyn_sol_value(&decoded, output_type)
}

/// Convert DynSolValue to DecodedValue
fn convert_dyn_sol_value(
    value: &DynSolValue,
    output_type: &EvmType,
) -> Result<DecodedValue, EthCallDecodingError> {
    // Handle Named types by delegating to inner type
    if let EvmType::Named { inner, .. } = output_type {
        return convert_dyn_sol_value(value, inner);
    }

    // Handle NamedTuple types
    if let EvmType::NamedTuple(fields) = output_type {
        if let DynSolValue::Tuple(values) = value {
            if values.len() != fields.len() {
                return Err(EthCallDecodingError::Decode(format!(
                    "Tuple length mismatch: expected {}, got {}",
                    fields.len(),
                    values.len()
                )));
            }
            let mut named_values = Vec::with_capacity(fields.len());
            for ((name, field_type), val) in fields.iter().zip(values.iter()) {
                let decoded = convert_dyn_sol_value(val, field_type)?;
                named_values.push((name.clone(), decoded));
            }
            return Ok(DecodedValue::NamedTuple(named_values));
        } else {
            return Err(EthCallDecodingError::Decode(format!(
                "Expected tuple value for NamedTuple type, got {:?}",
                value
            )));
        }
    }

    // Handle UnnamedTuple types
    if let EvmType::UnnamedTuple(field_types) = output_type {
        if let DynSolValue::Tuple(values) = value {
            if values.len() != field_types.len() {
                return Err(EthCallDecodingError::Decode(format!(
                    "Tuple length mismatch: expected {}, got {}",
                    field_types.len(),
                    values.len()
                )));
            }
            let decoded: Vec<DecodedValue> = field_types
                .iter()
                .zip(values.iter())
                .map(|(ty, val)| convert_dyn_sol_value(val, ty))
                .collect::<Result<_, _>>()?;
            return Ok(DecodedValue::UnnamedTuple(decoded));
        } else {
            return Err(EthCallDecodingError::Decode(format!(
                "Expected tuple value for UnnamedTuple type, got {:?}",
                value
            )));
        }
    }

    // Handle Array types
    if let EvmType::Array(inner_type) = output_type {
        if let DynSolValue::Array(values) = value {
            let decoded: Vec<DecodedValue> = values
                .iter()
                .map(|v| convert_dyn_sol_value(v, inner_type))
                .collect::<Result<_, _>>()?;
            return Ok(DecodedValue::Array(decoded));
        } else {
            return Err(EthCallDecodingError::Decode(format!(
                "Expected array value for Array type, got {:?}",
                value
            )));
        }
    }

    match value {
        DynSolValue::Address(addr) => Ok(DecodedValue::Address(addr.0 .0)),
        DynSolValue::Uint(val, _) => match output_type {
            EvmType::Uint8 => {
                let v: u8 = (*val).try_into().map_err(|_| {
                    EthCallDecodingError::Decode(format!("Uint value {} overflows u8", val))
                })?;
                Ok(DecodedValue::Uint8(v))
            }
            EvmType::Uint64 | EvmType::Uint32 | EvmType::Uint24 | EvmType::Uint16 => {
                let v: u64 = (*val).try_into().map_err(|_| {
                    EthCallDecodingError::Decode(format!("Uint value {} overflows u64", val))
                })?;
                Ok(DecodedValue::Uint64(v))
            }
            _ => Ok(DecodedValue::Uint256(*val)),
        },
        DynSolValue::Int(val, _) => match output_type {
            EvmType::Int8 => {
                let v: i8 = (*val).try_into().map_err(|_| {
                    EthCallDecodingError::Decode(format!("Int value {} overflows i8", val))
                })?;
                Ok(DecodedValue::Int8(v))
            }
            EvmType::Int64 | EvmType::Int32 | EvmType::Int24 | EvmType::Int16 => {
                let v: i64 = (*val).try_into().map_err(|_| {
                    EthCallDecodingError::Decode(format!("Int value {} overflows i64", val))
                })?;
                Ok(DecodedValue::Int64(v))
            }
            _ => Ok(DecodedValue::Int256(*val)),
        },
        DynSolValue::Bool(b) => Ok(DecodedValue::Bool(*b)),
        DynSolValue::FixedBytes(bytes, 32) => {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&bytes[..]);
            Ok(DecodedValue::Bytes32(arr))
        }
        DynSolValue::FixedBytes(bytes, _) => Ok(DecodedValue::Bytes(bytes.to_vec())),
        DynSolValue::Bytes(bytes) => Ok(DecodedValue::Bytes(bytes.clone())),
        DynSolValue::String(s) => Ok(DecodedValue::String(s.clone())),
        _ => Err(EthCallDecodingError::Decode(format!(
            "Unsupported value type: {:?}",
            value
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{I256, U256};

    #[test]
    fn test_uint8_overflow_returns_error() {
        let val = DynSolValue::Uint(U256::from(256u64), 8);
        let result = convert_dyn_sol_value(&val, &EvmType::Uint8);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("overflows u8"),
            "Expected overflow error, got: {}",
            err
        );
    }

    #[test]
    fn test_uint64_overflow_returns_error() {
        // U256::MAX is well above u64::MAX
        let val = DynSolValue::Uint(U256::MAX, 256);
        let result = convert_dyn_sol_value(&val, &EvmType::Uint64);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("overflows u64"),
            "Expected overflow error, got: {}",
            err
        );
    }

    #[test]
    fn test_int8_overflow_returns_error() {
        // 128 is outside i8 range [-128, 127]
        let val = DynSolValue::Int(I256::try_from(128i64).unwrap(), 8);
        let result = convert_dyn_sol_value(&val, &EvmType::Int8);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("overflows i8"),
            "Expected overflow error, got: {}",
            err
        );
    }

    #[test]
    fn test_int64_overflow_returns_error() {
        // I256::MAX is well above i64::MAX
        let val = DynSolValue::Int(I256::MAX, 256);
        let result = convert_dyn_sol_value(&val, &EvmType::Int64);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("overflows i64"),
            "Expected overflow error, got: {}",
            err
        );
    }

    #[test]
    fn test_valid_values_still_decode() {
        // Valid u8
        let val = DynSolValue::Uint(U256::from(255u64), 8);
        let result = convert_dyn_sol_value(&val, &EvmType::Uint8).unwrap();
        assert!(matches!(result, DecodedValue::Uint8(255)));

        // Valid u64
        let val = DynSolValue::Uint(U256::from(u64::MAX), 64);
        let result = convert_dyn_sol_value(&val, &EvmType::Uint64).unwrap();
        assert!(matches!(result, DecodedValue::Uint64(v) if v == u64::MAX));

        // Valid i8
        let val = DynSolValue::Int(I256::try_from(-128i64).unwrap(), 8);
        let result = convert_dyn_sol_value(&val, &EvmType::Int8).unwrap();
        assert!(matches!(result, DecodedValue::Int8(-128)));

        // Valid i64
        let val = DynSolValue::Int(I256::try_from(i64::MAX).unwrap(), 64);
        let result = convert_dyn_sol_value(&val, &EvmType::Int64).unwrap();
        assert!(matches!(result, DecodedValue::Int64(v) if v == i64::MAX));

        // Uint256 passthrough
        let val = DynSolValue::Uint(U256::MAX, 256);
        let result = convert_dyn_sol_value(&val, &EvmType::Uint256).unwrap();
        assert!(matches!(result, DecodedValue::Uint256(v) if v == U256::MAX));

        // Int256 passthrough
        let val = DynSolValue::Int(I256::MIN, 256);
        let result = convert_dyn_sol_value(&val, &EvmType::Int256).unwrap();
        assert!(matches!(result, DecodedValue::Int256(v) if v == I256::MIN));

        // Zero values
        let val = DynSolValue::Uint(U256::ZERO, 8);
        let result = convert_dyn_sol_value(&val, &EvmType::Uint8).unwrap();
        assert!(matches!(result, DecodedValue::Uint8(0)));

        let val = DynSolValue::Int(I256::ZERO, 8);
        let result = convert_dyn_sol_value(&val, &EvmType::Int8).unwrap();
        assert!(matches!(result, DecodedValue::Int8(0)));
    }
}
