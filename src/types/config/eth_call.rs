use alloy::dyn_abi::DynSolValue;
use alloy::primitives::{Address, Bytes, B256, I256, U256};
use arrow::datatypes::DataType;
use serde::de::{self, Visitor};
use serde::Deserialize;
use std::fmt;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ParamError {
    #[error("Invalid address format: {0}")]
    InvalidAddress(String),

    #[error("Invalid number format: {0}")]
    InvalidNumber(String),

    #[error("Invalid hex format: {0}")]
    InvalidHex(String),

    #[error("Type mismatch: expected {expected}, got {got}")]
    TypeMismatch { expected: String, got: String },
}

#[derive(Debug, Error)]
pub enum FrequencyError {
    #[error("Invalid duration format: {0}")]
    InvalidDuration(String),
}

/// Frequency at which to make eth_calls
#[derive(Debug, Clone, PartialEq)]
pub enum Frequency {
    /// Call every block (default)
    EveryBlock,
    /// Call once per address (at discovery for factory, at start_block for regular)
    Once,
    /// Call every N blocks
    EveryNBlocks(u64),
    /// Call at time intervals (stored as seconds)
    Duration(u64),
}

impl Default for Frequency {
    fn default() -> Self {
        Frequency::EveryBlock
    }
}

impl Frequency {
    pub fn is_once(&self) -> bool {
        matches!(self, Frequency::Once)
    }

    fn parse_duration_string(s: &str) -> Result<u64, FrequencyError> {
        let s = s.trim().to_lowercase();

        if let Some(num_str) = s.strip_suffix('s') {
            return num_str
                .parse::<u64>()
                .map_err(|_| FrequencyError::InvalidDuration(s.clone()));
        }
        if let Some(num_str) = s.strip_suffix('m') {
            return num_str
                .parse::<u64>()
                .map(|n| n * 60)
                .map_err(|_| FrequencyError::InvalidDuration(s.clone()));
        }
        if let Some(num_str) = s.strip_suffix('h') {
            return num_str
                .parse::<u64>()
                .map(|n| n * 3600)
                .map_err(|_| FrequencyError::InvalidDuration(s.clone()));
        }
        if let Some(num_str) = s.strip_suffix('d') {
            return num_str
                .parse::<u64>()
                .map(|n| n * 86400)
                .map_err(|_| FrequencyError::InvalidDuration(s.clone()));
        }

        Err(FrequencyError::InvalidDuration(s))
    }
}

impl<'de> Deserialize<'de> for Frequency {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct FrequencyVisitor;

        impl<'de> Visitor<'de> for FrequencyVisitor {
            type Value = Frequency;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str(
                    "\"once\", a positive integer, or a duration string like \"5m\", \"1h\", \"1d\"",
                )
            }

            fn visit_str<E>(self, value: &str) -> Result<Frequency, E>
            where
                E: de::Error,
            {
                let lower = value.to_lowercase();
                if lower == "once" {
                    return Ok(Frequency::Once);
                }

                if lower.ends_with('s')
                    || lower.ends_with('m')
                    || lower.ends_with('h')
                    || lower.ends_with('d')
                {
                    let secs = Frequency::parse_duration_string(value).map_err(de::Error::custom)?;
                    return Ok(Frequency::Duration(secs));
                }

                if let Ok(n) = value.parse::<u64>() {
                    if n == 0 {
                        return Err(de::Error::custom("frequency must be positive"));
                    }
                    return Ok(Frequency::EveryNBlocks(n));
                }

                Err(de::Error::custom(format!(
                    "invalid frequency: expected \"once\", number, or duration like \"5m\", got \"{}\"",
                    value
                )))
            }

            fn visit_u64<E>(self, value: u64) -> Result<Frequency, E>
            where
                E: de::Error,
            {
                if value == 0 {
                    return Err(de::Error::custom("frequency must be positive"));
                }
                Ok(Frequency::EveryNBlocks(value))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Frequency, E>
            where
                E: de::Error,
            {
                if value <= 0 {
                    return Err(de::Error::custom("frequency must be positive"));
                }
                Ok(Frequency::EveryNBlocks(value as u64))
            }
        }

        deserializer.deserialize_any(FrequencyVisitor)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct EthCallConfig {
    pub function: String,
    pub output_type: EvmType,
    #[serde(default)]
    pub params: Vec<ParamConfig>,
    #[serde(default)]
    pub frequency: Frequency,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ParamConfig {
    #[serde(rename = "type")]
    pub param_type: EvmType,
    pub values: Vec<ParamValue>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum ParamValue {
    String(String),
    Number(u64),
    Bool(bool),
}

impl ParamValue {
    pub fn as_string(&self) -> Result<String, ParamError> {
        match self {
            ParamValue::String(s) => Ok(s.clone()),
            ParamValue::Number(n) => Ok(n.to_string()),
            ParamValue::Bool(b) => Ok(b.to_string()),
        }
    }

    pub fn as_bool(&self) -> Result<bool, ParamError> {
        match self {
            ParamValue::Bool(b) => Ok(*b),
            ParamValue::String(s) => match s.to_lowercase().as_str() {
                "true" | "1" => Ok(true),
                "false" | "0" => Ok(false),
                _ => Err(ParamError::TypeMismatch {
                    expected: "bool".to_string(),
                    got: s.clone(),
                }),
            },
            ParamValue::Number(n) => Ok(*n != 0),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EvmType {
    Int256,
    Int128,
    Int64,
    Int32,
    Int8,
    Uint256,
    Uint128,
    Uint80,
    Uint64,
    Uint32,
    Uint16,
    Uint8,
    Address,
    Bool,
    Bytes32,
    Bytes,
    String,
}

impl EvmType {
    pub fn parse_value(&self, value: &ParamValue) -> Result<DynSolValue, ParamError> {
        match self {
            EvmType::Uint256 => {
                let s = value.as_string()?;
                let val = parse_uint256(&s)?;
                Ok(DynSolValue::Uint(val, 256))
            }
            EvmType::Uint128 => {
                let s = value.as_string()?;
                let val = parse_uint256(&s)?;
                Ok(DynSolValue::Uint(val, 128))
            }
            EvmType::Uint80 => {
                let s = value.as_string()?;
                let val = parse_uint256(&s)?;
                Ok(DynSolValue::Uint(val, 80))
            }
            EvmType::Uint64 => {
                let s = value.as_string()?;
                let val = parse_uint256(&s)?;
                Ok(DynSolValue::Uint(val, 64))
            }
            EvmType::Uint32 => {
                let s = value.as_string()?;
                let val = parse_uint256(&s)?;
                Ok(DynSolValue::Uint(val, 32))
            }
            EvmType::Uint16 => {
                let s = value.as_string()?;
                let val = parse_uint256(&s)?;
                Ok(DynSolValue::Uint(val, 16))
            }
            EvmType::Uint8 => {
                let s = value.as_string()?;
                let val = parse_uint256(&s)?;
                Ok(DynSolValue::Uint(val, 8))
            }
            EvmType::Int256 => {
                let s = value.as_string()?;
                let val = parse_int256(&s)?;
                Ok(DynSolValue::Int(val, 256))
            }
            EvmType::Int128 => {
                let s = value.as_string()?;
                let val = parse_int256(&s)?;
                Ok(DynSolValue::Int(val, 128))
            }
            EvmType::Int64 => {
                let s = value.as_string()?;
                let val = parse_int256(&s)?;
                Ok(DynSolValue::Int(val, 64))
            }
            EvmType::Int32 => {
                let s = value.as_string()?;
                let val = parse_int256(&s)?;
                Ok(DynSolValue::Int(val, 32))
            }
            EvmType::Int8 => {
                let s = value.as_string()?;
                let val = parse_int256(&s)?;
                Ok(DynSolValue::Int(val, 8))
            }
            EvmType::Address => {
                let s = value.as_string()?;
                let addr = s
                    .parse::<Address>()
                    .map_err(|_| ParamError::InvalidAddress(s))?;
                Ok(DynSolValue::Address(addr))
            }
            EvmType::Bool => {
                let b = value.as_bool()?;
                Ok(DynSolValue::Bool(b))
            }
            EvmType::Bytes32 => {
                let s = value.as_string()?;
                let bytes = s
                    .parse::<B256>()
                    .map_err(|_| ParamError::InvalidHex(s))?;
                Ok(DynSolValue::FixedBytes(bytes, 32))
            }
            EvmType::Bytes => {
                let s = value.as_string()?;
                let hex_str = s.strip_prefix("0x").unwrap_or(&s);
                let bytes =
                    hex::decode(hex_str).map_err(|_| ParamError::InvalidHex(s))?;
                Ok(DynSolValue::Bytes(bytes))
            }
            EvmType::String => {
                let s = value.as_string()?;
                Ok(DynSolValue::String(s))
            }
        }
    }

    /// Convert EvmType to Arrow DataType for parquet writing
    pub fn to_arrow_type(&self) -> DataType {
        match self {
            // Large integers stored as strings to preserve precision
            EvmType::Int256 | EvmType::Int128 => DataType::Utf8,
            EvmType::Int64 => DataType::Int64,
            EvmType::Int32 => DataType::Int32,
            EvmType::Int8 => DataType::Int8,
            // Large unsigned integers stored as strings to preserve precision
            EvmType::Uint256 | EvmType::Uint128 | EvmType::Uint80 => DataType::Utf8,
            EvmType::Uint64 => DataType::UInt64,
            EvmType::Uint32 => DataType::UInt32,
            EvmType::Uint16 => DataType::UInt16,
            EvmType::Uint8 => DataType::UInt8,
            EvmType::Address => DataType::FixedSizeBinary(20),
            EvmType::Bool => DataType::Boolean,
            EvmType::Bytes32 => DataType::FixedSizeBinary(32),
            EvmType::Bytes => DataType::Binary,
            EvmType::String => DataType::Utf8,
        }
    }
}

fn parse_uint256(s: &str) -> Result<U256, ParamError> {
    let s = s.trim();
    if s.starts_with("0x") || s.starts_with("0X") {
        U256::from_str_radix(&s[2..], 16).map_err(|_| ParamError::InvalidNumber(s.to_string()))
    } else {
        U256::from_str_radix(s, 10).map_err(|_| ParamError::InvalidNumber(s.to_string()))
    }
}

fn parse_int256(s: &str) -> Result<I256, ParamError> {
    let s = s.trim();
    let is_negative = s.starts_with('-');
    let s = if is_negative { &s[1..] } else { s };

    let abs_val = parse_uint256(s)?;

    if is_negative {
        Ok(-I256::try_from(abs_val).map_err(|_| ParamError::InvalidNumber(s.to_string()))?)
    } else {
        I256::try_from(abs_val).map_err(|_| ParamError::InvalidNumber(s.to_string()))
    }
}

pub fn encode_call_with_params(
    function_selector: [u8; 4],
    params: &[DynSolValue],
) -> Bytes {
    if params.is_empty() {
        return Bytes::copy_from_slice(&function_selector);
    }

    let encoded_params = DynSolValue::Tuple(params.to_vec()).abi_encode_params();

    let mut calldata = Vec::with_capacity(4 + encoded_params.len());
    calldata.extend_from_slice(&function_selector);
    calldata.extend_from_slice(&encoded_params);

    Bytes::from(calldata)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_address() {
        let value = ParamValue::String("0x1234567890abcdef1234567890abcdef12345678".to_string());
        let result = EvmType::Address.parse_value(&value).unwrap();
        assert!(matches!(result, DynSolValue::Address(_)));
    }

    #[test]
    fn test_parse_uint256() {
        let value = ParamValue::String("1000000000000000000".to_string());
        let result = EvmType::Uint256.parse_value(&value).unwrap();
        assert!(matches!(result, DynSolValue::Uint(_, 256)));
    }

    #[test]
    fn test_parse_uint256_hex() {
        let value = ParamValue::String("0xde0b6b3a7640000".to_string());
        let result = EvmType::Uint256.parse_value(&value).unwrap();
        if let DynSolValue::Uint(v, _) = result {
            assert_eq!(v, U256::from(1000000000000000000u64));
        } else {
            panic!("Expected Uint");
        }
    }

    #[test]
    fn test_parse_bool() {
        let value = ParamValue::Bool(true);
        let result = EvmType::Bool.parse_value(&value).unwrap();
        assert!(matches!(result, DynSolValue::Bool(true)));
    }

    #[test]
    fn test_encode_call_no_params() {
        let selector = [0x18, 0x16, 0x0d, 0xdd];
        let result = encode_call_with_params(selector, &[]);
        assert_eq!(result.len(), 4);
        assert_eq!(&result[..], &selector);
    }

    #[test]
    fn test_encode_call_with_address() {
        let selector = [0x70, 0xa0, 0x82, 0x31];
        let addr = "0x1234567890abcdef1234567890abcdef12345678"
            .parse::<Address>()
            .unwrap();
        let params = vec![DynSolValue::Address(addr)];
        let result = encode_call_with_params(selector, &params);
        assert_eq!(result.len(), 4 + 32);
    }

    #[test]
    fn test_frequency_deserialize_once() {
        let json = r#""once""#;
        let freq: Frequency = serde_json::from_str(json).unwrap();
        assert_eq!(freq, Frequency::Once);
    }

    #[test]
    fn test_frequency_deserialize_blocks() {
        let json = r#"100"#;
        let freq: Frequency = serde_json::from_str(json).unwrap();
        assert_eq!(freq, Frequency::EveryNBlocks(100));
    }

    #[test]
    fn test_frequency_deserialize_duration_minutes() {
        let json = r#""5m""#;
        let freq: Frequency = serde_json::from_str(json).unwrap();
        assert_eq!(freq, Frequency::Duration(300));
    }

    #[test]
    fn test_frequency_deserialize_duration_hours() {
        let json = r#""1h""#;
        let freq: Frequency = serde_json::from_str(json).unwrap();
        assert_eq!(freq, Frequency::Duration(3600));
    }

    #[test]
    fn test_frequency_deserialize_duration_days() {
        let json = r#""1d""#;
        let freq: Frequency = serde_json::from_str(json).unwrap();
        assert_eq!(freq, Frequency::Duration(86400));
    }

    #[test]
    fn test_frequency_default() {
        assert_eq!(Frequency::default(), Frequency::EveryBlock);
    }

    #[test]
    fn test_eth_call_config_with_frequency() {
        let json = r#"{
            "function": "name()",
            "output_type": "string",
            "frequency": "once"
        }"#;
        let config: EthCallConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.function, "name()");
        assert_eq!(config.frequency, Frequency::Once);
    }

    #[test]
    fn test_eth_call_config_default_frequency() {
        let json = r#"{
            "function": "latestAnswer()",
            "output_type": "int256"
        }"#;
        let config: EthCallConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.frequency, Frequency::EveryBlock);
    }
}
