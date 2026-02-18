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

#[derive(Debug, Error)]
pub enum EvmTypeParseError {
    #[error("Unknown EVM type: {0}")]
    UnknownType(String),
    #[error("Invalid tuple format: {0}")]
    InvalidTuple(String),
    #[error("Empty tuple field")]
    EmptyField,
}

/// Configuration for event-triggered eth_calls
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct EventTriggerConfig {
    /// Contract name or factory collection name that emits the trigger event
    pub source: String,
    /// Event signature (e.g., "Transfer(address,address,uint256)")
    pub event: String,
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
    /// Call when specific events are emitted
    OnEvents(EventTriggerConfig),
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

    pub fn is_on_events(&self) -> bool {
        matches!(self, Frequency::OnEvents(_))
    }

    pub fn as_on_events(&self) -> Option<&EventTriggerConfig> {
        match self {
            Frequency::OnEvents(config) => Some(config),
            _ => None,
        }
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
                    "\"once\", a positive integer, a duration string like \"5m\", or {\"on_events\": {...}}",
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

            fn visit_map<M>(self, mut map: M) -> Result<Frequency, M::Error>
            where
                M: de::MapAccess<'de>,
            {
                let mut on_events: Option<EventTriggerConfig> = None;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "on_events" => {
                            if on_events.is_some() {
                                return Err(de::Error::duplicate_field("on_events"));
                            }
                            on_events = Some(map.next_value()?);
                        }
                        _ => {
                            return Err(de::Error::unknown_field(&key, &["on_events"]));
                        }
                    }
                }

                match on_events {
                    Some(config) => Ok(Frequency::OnEvents(config)),
                    None => Err(de::Error::missing_field("on_events")),
                }
            }
        }

        deserializer.deserialize_any(FrequencyVisitor)
    }
}

/// Optional target override for eth_calls.
/// When specified, the call is made to this address instead of the contract it's configured under.
#[derive(Debug, Clone)]
pub enum CallTarget {
    /// A direct hex address (e.g., "0x1234...")
    Address(Address),
    /// A contract name to look up from the chain's contract configs
    Name(std::string::String),
}

impl CallTarget {
    /// Resolve this target to an address, looking up contract names from the provided contracts map.
    pub fn resolve(&self, contracts: &std::collections::HashMap<std::string::String, super::contract::ContractConfig>) -> Option<Address> {
        match self {
            CallTarget::Address(addr) => Some(*addr),
            CallTarget::Name(name) => {
                let contract = contracts.get(name)?;
                match &contract.address {
                    super::contract::AddressOrAddresses::Single(addr) => Some(*addr),
                    super::contract::AddressOrAddresses::Multiple(addrs) => addrs.first().copied(),
                }
            }
        }
    }

    /// Resolve this target to all addresses (for multi-address contracts).
    pub fn resolve_all(&self, contracts: &std::collections::HashMap<std::string::String, super::contract::ContractConfig>) -> Option<Vec<Address>> {
        match self {
            CallTarget::Address(addr) => Some(vec![*addr]),
            CallTarget::Name(name) => {
                let contract = contracts.get(name)?;
                match &contract.address {
                    super::contract::AddressOrAddresses::Single(addr) => Some(vec![*addr]),
                    super::contract::AddressOrAddresses::Multiple(addrs) => Some(addrs.clone()),
                }
            }
        }
    }
}

impl<'de> Deserialize<'de> for CallTarget {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = std::string::String::deserialize(deserializer)?;
        // Try parsing as an address first (starts with 0x and is 42 chars)
        if let Ok(addr) = s.parse::<Address>() {
            Ok(CallTarget::Address(addr))
        } else {
            Ok(CallTarget::Name(s))
        }
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
    /// Optional target address override. Can be a hex address or a contract name to look up.
    /// When omitted, calls are made to the contract this config is nested under.
    #[serde(default)]
    pub target: Option<CallTarget>,
}

impl EthCallConfig {
    /// Check if any parameter uses self-address (source: "self")
    pub fn has_self_address_param(&self) -> bool {
        self.params.iter().any(|p| p.is_self_address())
    }
}

/// Parameter configuration for eth_calls
/// Supports static values, event data binding, and self-address for factory collections
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum ParamConfig {
    /// Static values (original behavior): {"type": "address", "values": ["0x..."]}
    Static {
        #[serde(rename = "type")]
        param_type: EvmType,
        values: Vec<ParamValue>,
    },
    /// Bind from event data (for on_events frequency): {"type": "address", "from_event": "topics[1]"}
    FromEvent {
        #[serde(rename = "type")]
        param_type: EvmType,
        from_event: String,
    },
    /// Self address (event emitter, for factory collections): {"type": "address", "source": "self"}
    SelfAddress {
        #[serde(rename = "type")]
        param_type: EvmType,
        source: String,
    },
}

impl ParamConfig {
    /// Get the parameter type
    pub fn param_type(&self) -> &EvmType {
        match self {
            ParamConfig::Static { param_type, .. } => param_type,
            ParamConfig::FromEvent { param_type, .. } => param_type,
            ParamConfig::SelfAddress { param_type, .. } => param_type,
        }
    }

    /// Get static values if this is a Static param config
    pub fn values(&self) -> Option<&Vec<ParamValue>> {
        match self {
            ParamConfig::Static { values, .. } => Some(values),
            _ => None,
        }
    }

    /// Get the event field reference if this is a FromEvent param config
    pub fn from_event(&self) -> Option<&str> {
        match self {
            ParamConfig::FromEvent { from_event, .. } => Some(from_event),
            _ => None,
        }
    }

    /// Check if this is a self-address param (source: "self")
    pub fn is_self_address(&self) -> bool {
        matches!(self, ParamConfig::SelfAddress { source, .. } if source == "self")
    }
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

/// EVM type representation for eth_call output types
/// Supports simple types, named single values, and named tuples
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EvmType {
    // Simple types (unnamed)
    Int256,
    Int128,
    Int64,
    Int32,
    Int24,
    Int16,
    Int8,
    Uint256,
    Uint160,
    Uint128,
    Uint96,
    Uint80,
    Uint64,
    Uint32,
    Uint24,
    Uint16,
    Uint8,
    Address,
    Bool,
    Bytes32,
    Bytes,
    String,
    /// Named single value: "int256 latestAnswer" → column "latestAnswer"
    Named {
        name: std::string::String,
        inner: Box<EvmType>,
    },
    /// Named tuple: "(uint160 sqrtPriceX96, int24 tick)" → columns "sqrtPriceX96", "tick"
    NamedTuple(Vec<(std::string::String, Box<EvmType>)>),
}

impl EvmType {
    /// Parse a simple type string (no name) into EvmType
    fn parse_simple(s: &str) -> Result<EvmType, EvmTypeParseError> {
        match s.to_lowercase().as_str() {
            "int256" => Ok(EvmType::Int256),
            "int128" => Ok(EvmType::Int128),
            "int64" => Ok(EvmType::Int64),
            "int32" => Ok(EvmType::Int32),
            "int24" => Ok(EvmType::Int24),
            "int16" => Ok(EvmType::Int16),
            "int8" => Ok(EvmType::Int8),
            "uint256" => Ok(EvmType::Uint256),
            "uint160" => Ok(EvmType::Uint160),
            "uint128" => Ok(EvmType::Uint128),
            "uint96" => Ok(EvmType::Uint96),
            "uint80" => Ok(EvmType::Uint80),
            "uint64" => Ok(EvmType::Uint64),
            "uint32" => Ok(EvmType::Uint32),
            "uint24" => Ok(EvmType::Uint24),
            "uint16" => Ok(EvmType::Uint16),
            "uint8" => Ok(EvmType::Uint8),
            "address" => Ok(EvmType::Address),
            "bool" => Ok(EvmType::Bool),
            "bytes32" => Ok(EvmType::Bytes32),
            "bytes" => Ok(EvmType::Bytes),
            "string" => Ok(EvmType::String),
            _ => Err(EvmTypeParseError::UnknownType(s.to_string())),
        }
    }

    /// Parse an output_type string into EvmType
    /// Handles: "uint256", "int256 latestAnswer", "(uint160 sqrtPriceX96, int24 tick)"
    pub fn parse(s: &str) -> Result<EvmType, EvmTypeParseError> {
        let s = s.trim();

        // Check if it's a tuple
        if s.starts_with('(') && s.ends_with(')') {
            return Self::parse_named_tuple(s);
        }

        // Check if it's a named single value (contains space but not a tuple)
        if let Some(space_idx) = s.find(' ') {
            let type_str = &s[..space_idx].trim();
            let name = &s[space_idx + 1..].trim();
            if !name.is_empty() {
                let inner = Self::parse_simple(type_str)?;
                return Ok(EvmType::Named {
                    name: name.to_string(),
                    inner: Box::new(inner),
                });
            }
        }

        // Simple type
        Self::parse_simple(s)
    }

    /// Parse a named tuple like "(uint160 sqrtPriceX96, int24 tick)"
    fn parse_named_tuple(s: &str) -> Result<EvmType, EvmTypeParseError> {
        // Strip outer parentheses
        let inner = s
            .strip_prefix('(')
            .and_then(|s| s.strip_suffix(')'))
            .ok_or_else(|| EvmTypeParseError::InvalidTuple(s.to_string()))?;

        if inner.trim().is_empty() {
            return Err(EvmTypeParseError::InvalidTuple("empty tuple".to_string()));
        }

        // Split by comma, respecting nested parentheses
        let fields = split_tuple_fields(inner)?;

        let mut parsed_fields = Vec::new();
        for field in fields {
            let field = field.trim();
            if field.is_empty() {
                return Err(EvmTypeParseError::EmptyField);
            }

            // Each field is "type name"
            let parts: Vec<&str> = field.splitn(2, ' ').collect();
            if parts.len() != 2 {
                return Err(EvmTypeParseError::InvalidTuple(format!(
                    "field '{}' must have type and name",
                    field
                )));
            }

            let type_str = parts[0].trim();
            let name = parts[1].trim();

            if name.is_empty() {
                return Err(EvmTypeParseError::InvalidTuple(format!(
                    "field '{}' has empty name",
                    field
                )));
            }

            let field_type = Self::parse_simple(type_str)?;
            parsed_fields.push((name.to_string(), Box::new(field_type)));
        }

        Ok(EvmType::NamedTuple(parsed_fields))
    }

    /// Get the column name for named single values
    pub fn column_name(&self) -> Option<&str> {
        match self {
            EvmType::Named { name, .. } => Some(name),
            _ => None,
        }
    }

    /// Get field names for named tuples
    pub fn field_names(&self) -> Option<Vec<&str>> {
        match self {
            EvmType::NamedTuple(fields) => Some(fields.iter().map(|(n, _)| n.as_str()).collect()),
            _ => None,
        }
    }

    /// Get the inner type for Named variant, or self for simple types
    pub fn inner_type(&self) -> &EvmType {
        match self {
            EvmType::Named { inner, .. } => inner,
            _ => self,
        }
    }

    /// Check if this is a named tuple
    pub fn is_named_tuple(&self) -> bool {
        matches!(self, EvmType::NamedTuple(_))
    }

    /// Check if this is a named single value
    pub fn is_named(&self) -> bool {
        matches!(self, EvmType::Named { .. })
    }
}

/// Split tuple fields by comma, respecting nested parentheses
fn split_tuple_fields(s: &str) -> Result<Vec<&str>, EvmTypeParseError> {
    let mut fields = Vec::new();
    let mut depth = 0;
    let mut start = 0;

    for (i, c) in s.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                fields.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }

    // Add the last field
    if start < s.len() {
        fields.push(&s[start..]);
    }

    Ok(fields)
}

impl<'de> Deserialize<'de> for EvmType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct EvmTypeVisitor;

        impl<'de> Visitor<'de> for EvmTypeVisitor {
            type Value = EvmType;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str(
                    "an EVM type like \"uint256\", \"int256 latestAnswer\", or \"(uint160 sqrtPriceX96, int24 tick)\"",
                )
            }

            fn visit_str<E>(self, value: &str) -> Result<EvmType, E>
            where
                E: de::Error,
            {
                EvmType::parse(value).map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_str(EvmTypeVisitor)
    }
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
            EvmType::Uint24 => {
                let s = value.as_string()?;
                let val = parse_uint256(&s)?;
                Ok(DynSolValue::Uint(val, 24))
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
            EvmType::Int24 => {
                let s = value.as_string()?;
                let val = parse_int256(&s)?;
                Ok(DynSolValue::Int(val, 24))
            }
            EvmType::Int16 => {
                let s = value.as_string()?;
                let val = parse_int256(&s)?;
                Ok(DynSolValue::Int(val, 16))
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
            EvmType::Uint160 => {
                let s = value.as_string()?;
                let val = parse_uint256(&s)?;
                Ok(DynSolValue::Uint(val, 160))
            }
            EvmType::Uint96 => {
                let s = value.as_string()?;
                let val = parse_uint256(&s)?;
                Ok(DynSolValue::Uint(val, 96))
            }
            // Named types delegate to their inner type
            EvmType::Named { inner, .. } => inner.parse_value(value),
            // NamedTuple doesn't support parsing from ParamValue
            EvmType::NamedTuple(_) => Err(ParamError::TypeMismatch {
                expected: "simple type".to_string(),
                got: "named tuple".to_string(),
            }),
        }
    }

    /// Convert EvmType to Arrow DataType for parquet writing
    pub fn to_arrow_type(&self) -> DataType {
        match self {
            // Large integers stored as strings to preserve precision
            EvmType::Int256 | EvmType::Int128 => DataType::Utf8,
            EvmType::Int64 => DataType::Int64,
            EvmType::Int32 | EvmType::Int24 => DataType::Int32,
            EvmType::Int16 => DataType::Int16,
            EvmType::Int8 => DataType::Int8,
            // Large unsigned integers stored as strings to preserve precision
            EvmType::Uint256 | EvmType::Uint160 | EvmType::Uint128 | EvmType::Uint96 | EvmType::Uint80 => DataType::Utf8,
            EvmType::Uint64 => DataType::UInt64,
            EvmType::Uint32 | EvmType::Uint24 => DataType::UInt32,
            EvmType::Uint16 => DataType::UInt16,
            EvmType::Uint8 => DataType::UInt8,
            EvmType::Address => DataType::FixedSizeBinary(20),
            EvmType::Bool => DataType::Boolean,
            EvmType::Bytes32 => DataType::FixedSizeBinary(32),
            EvmType::Bytes => DataType::Binary,
            EvmType::String => DataType::Utf8,
            // Named types delegate to their inner type
            EvmType::Named { inner, .. } => inner.to_arrow_type(),
            // NamedTuple - should use field_arrow_types() instead
            EvmType::NamedTuple(_) => DataType::Utf8, // Fallback, shouldn't be used directly
        }
    }

    /// Get Arrow types for each field in a named tuple
    pub fn field_arrow_types(&self) -> Option<Vec<(&str, DataType)>> {
        match self {
            EvmType::NamedTuple(fields) => Some(
                fields
                    .iter()
                    .map(|(name, ty)| (name.as_str(), ty.to_arrow_type()))
                    .collect(),
            ),
            _ => None,
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

    #[test]
    fn test_frequency_deserialize_on_events() {
        let json = r#"{"on_events": {"source": "Token", "event": "Transfer(address,address,uint256)"}}"#;
        let freq: Frequency = serde_json::from_str(json).unwrap();
        assert!(freq.is_on_events());
        let config = freq.as_on_events().unwrap();
        assert_eq!(config.source, "Token");
        assert_eq!(config.event, "Transfer(address,address,uint256)");
    }

    #[test]
    fn test_frequency_on_events_helpers() {
        let freq = Frequency::OnEvents(EventTriggerConfig {
            source: "Pool".to_string(),
            event: "Swap(address,address,int256,int256,uint160,uint128,int24)".to_string(),
        });
        assert!(freq.is_on_events());
        assert!(!freq.is_once());
        assert!(freq.as_on_events().is_some());
    }

    #[test]
    fn test_eth_call_config_with_on_events() {
        let json = r#"{
            "function": "slot0()",
            "output_type": "uint256",
            "frequency": {
                "on_events": {
                    "source": "V3Pool",
                    "event": "Swap(address,address,int256,int256,uint160,uint128,int24)"
                }
            }
        }"#;
        let config: EthCallConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.function, "slot0()");
        assert!(config.frequency.is_on_events());
        let trigger = config.frequency.as_on_events().unwrap();
        assert_eq!(trigger.source, "V3Pool");
    }

    #[test]
    fn test_param_config_static() {
        let json = r#"{"type": "address", "values": ["0x1234567890abcdef1234567890abcdef12345678"]}"#;
        let param: ParamConfig = serde_json::from_str(json).unwrap();
        assert_eq!(*param.param_type(), EvmType::Address);
        assert!(param.values().is_some());
        assert_eq!(param.values().unwrap().len(), 1);
        assert!(param.from_event().is_none());
        assert!(!param.is_self_address());
    }

    #[test]
    fn test_param_config_from_event() {
        let json = r#"{"type": "address", "from_event": "topics[1]"}"#;
        let param: ParamConfig = serde_json::from_str(json).unwrap();
        assert_eq!(*param.param_type(), EvmType::Address);
        assert!(param.values().is_none());
        assert_eq!(param.from_event(), Some("topics[1]"));
        assert!(!param.is_self_address());
    }

    #[test]
    fn test_param_config_from_event_data() {
        let json = r#"{"type": "uint256", "from_event": "data[0]"}"#;
        let param: ParamConfig = serde_json::from_str(json).unwrap();
        assert_eq!(*param.param_type(), EvmType::Uint256);
        assert_eq!(param.from_event(), Some("data[0]"));
    }

    #[test]
    fn test_param_config_self_address() {
        let json = r#"{"type": "address", "source": "self"}"#;
        let param: ParamConfig = serde_json::from_str(json).unwrap();
        assert_eq!(*param.param_type(), EvmType::Address);
        assert!(param.values().is_none());
        assert!(param.from_event().is_none());
        assert!(param.is_self_address());
    }

    #[test]
    fn test_eth_call_config_with_from_event_param() {
        let json = r#"{
            "function": "balanceOf(address)",
            "output_type": "uint256",
            "frequency": {
                "on_events": {
                    "source": "Token",
                    "event": "Transfer(address,address,uint256)"
                }
            },
            "params": [
                {"type": "address", "from_event": "topics[2]"}
            ]
        }"#;
        let config: EthCallConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.function, "balanceOf(address)");
        assert!(config.frequency.is_on_events());
        assert_eq!(config.params.len(), 1);
        assert_eq!(config.params[0].from_event(), Some("topics[2]"));
    }

    #[test]
    fn test_evm_type_parse_simple() {
        assert_eq!(EvmType::parse("uint256").unwrap(), EvmType::Uint256);
        assert_eq!(EvmType::parse("int24").unwrap(), EvmType::Int24);
        assert_eq!(EvmType::parse("address").unwrap(), EvmType::Address);
        assert_eq!(EvmType::parse("bool").unwrap(), EvmType::Bool);
        assert_eq!(EvmType::parse("uint160").unwrap(), EvmType::Uint160);
    }

    #[test]
    fn test_evm_type_parse_named_single() {
        let result = EvmType::parse("int256 latestAnswer").unwrap();
        match result {
            EvmType::Named { name, inner } => {
                assert_eq!(name, "latestAnswer");
                assert_eq!(*inner, EvmType::Int256);
            }
            _ => panic!("Expected Named variant"),
        }
    }

    #[test]
    fn test_evm_type_parse_named_tuple() {
        let result = EvmType::parse("(uint160 sqrtPriceX96, int24 tick)").unwrap();
        match result {
            EvmType::NamedTuple(fields) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].0, "sqrtPriceX96");
                assert_eq!(*fields[0].1, EvmType::Uint160);
                assert_eq!(fields[1].0, "tick");
                assert_eq!(*fields[1].1, EvmType::Int24);
            }
            _ => panic!("Expected NamedTuple variant"),
        }
    }

    #[test]
    fn test_evm_type_parse_complex_tuple() {
        let result = EvmType::parse(
            "(uint160 sqrtPriceX96, int24 tick, uint16 observationIndex, uint16 observationCardinality, uint16 observationCardinalityNext, uint8 feeProtocol, bool unlocked)"
        ).unwrap();
        match result {
            EvmType::NamedTuple(fields) => {
                assert_eq!(fields.len(), 7);
                assert_eq!(fields[0].0, "sqrtPriceX96");
                assert_eq!(fields[6].0, "unlocked");
                assert_eq!(*fields[6].1, EvmType::Bool);
            }
            _ => panic!("Expected NamedTuple variant"),
        }
    }

    #[test]
    fn test_evm_type_deserialize_simple() {
        let json = r#""uint256""#;
        let result: EvmType = serde_json::from_str(json).unwrap();
        assert_eq!(result, EvmType::Uint256);
    }

    #[test]
    fn test_evm_type_deserialize_named_single() {
        let json = r#""int256 latestAnswer""#;
        let result: EvmType = serde_json::from_str(json).unwrap();
        assert!(result.is_named());
        assert_eq!(result.column_name(), Some("latestAnswer"));
    }

    #[test]
    fn test_evm_type_deserialize_named_tuple() {
        let json = r#""(uint160 sqrtPriceX96, int24 tick)""#;
        let result: EvmType = serde_json::from_str(json).unwrap();
        assert!(result.is_named_tuple());
        assert_eq!(result.field_names(), Some(vec!["sqrtPriceX96", "tick"]));
    }

    #[test]
    fn test_eth_call_config_with_named_output() {
        let json = r#"{
            "function": "latestAnswer()",
            "output_type": "int256 latestAnswer"
        }"#;
        let config: EthCallConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.function, "latestAnswer()");
        assert!(config.output_type.is_named());
        assert_eq!(config.output_type.column_name(), Some("latestAnswer"));
    }

    #[test]
    fn test_eth_call_config_with_tuple_output() {
        let json = r#"{
            "function": "slot0()",
            "output_type": "(uint160 sqrtPriceX96, int24 tick, uint16 observationIndex)"
        }"#;
        let config: EthCallConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.function, "slot0()");
        assert!(config.output_type.is_named_tuple());
        let names = config.output_type.field_names().unwrap();
        assert_eq!(names, vec!["sqrtPriceX96", "tick", "observationIndex"]);
    }

    #[test]
    fn test_eth_call_config_has_self_address_param() {
        // Config with self-address param
        let json = r#"{
            "function": "getAssetData(address)",
            "output_type": "uint256",
            "frequency": "once",
            "params": [{"type": "address", "source": "self"}]
        }"#;
        let config: EthCallConfig = serde_json::from_str(json).unwrap();
        assert!(config.has_self_address_param());

        // Config without self-address param
        let json_no_self = r#"{
            "function": "name()",
            "output_type": "string",
            "frequency": "once"
        }"#;
        let config_no_self: EthCallConfig = serde_json::from_str(json_no_self).unwrap();
        assert!(!config_no_self.has_self_address_param());

        // Config with static param only
        let json_static = r#"{
            "function": "balanceOf(address)",
            "output_type": "uint256",
            "params": [{"type": "address", "values": ["0x1234567890abcdef1234567890abcdef12345678"]}]
        }"#;
        let config_static: EthCallConfig = serde_json::from_str(json_static).unwrap();
        assert!(!config_static.has_self_address_param());
    }

    #[test]
    fn test_call_target_deserialize_address() {
        let json = r#""0x1234567890abcdef1234567890abcdef12345678""#;
        let target: CallTarget = serde_json::from_str(json).unwrap();
        assert!(matches!(target, CallTarget::Address(_)));
    }

    #[test]
    fn test_call_target_deserialize_name() {
        let json = r#""ChainlinkEthOracle""#;
        let target: CallTarget = serde_json::from_str(json).unwrap();
        match target {
            CallTarget::Name(name) => assert_eq!(name, "ChainlinkEthOracle"),
            _ => panic!("Expected Name variant"),
        }
    }

    #[test]
    fn test_eth_call_config_with_target_address() {
        let json = r#"{
            "function": "latestAnswer()",
            "output_type": "int256",
            "target": "0x1234567890abcdef1234567890abcdef12345678"
        }"#;
        let config: EthCallConfig = serde_json::from_str(json).unwrap();
        assert!(config.target.is_some());
        assert!(matches!(config.target.unwrap(), CallTarget::Address(_)));
    }

    #[test]
    fn test_eth_call_config_with_target_name() {
        let json = r#"{
            "function": "latestAnswer()",
            "output_type": "int256",
            "target": "ChainlinkEthOracle"
        }"#;
        let config: EthCallConfig = serde_json::from_str(json).unwrap();
        assert!(config.target.is_some());
        match config.target.unwrap() {
            CallTarget::Name(name) => assert_eq!(name, "ChainlinkEthOracle"),
            _ => panic!("Expected Name variant"),
        }
    }

    #[test]
    fn test_eth_call_config_without_target() {
        let json = r#"{
            "function": "latestAnswer()",
            "output_type": "int256"
        }"#;
        let config: EthCallConfig = serde_json::from_str(json).unwrap();
        assert!(config.target.is_none());
    }
}
