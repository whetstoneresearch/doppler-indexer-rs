use alloy::dyn_abi::DynSolValue;
use alloy::primitives::{Address, Bytes, B256, I256, U256};
use arrow::datatypes::{DataType, Field};
use serde::de::{self, Visitor};
use serde::Deserialize;
use std::fmt;
use std::sync::Arc;
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

/// Wrapper for event trigger configurations supporting single or multiple events
#[derive(Debug, Clone, PartialEq)]
pub enum EventTriggerConfigs {
    Single(EventTriggerConfig),
    Multiple(Vec<EventTriggerConfig>),
}

impl EventTriggerConfigs {
    /// Iterate over all event trigger configurations
    pub fn iter(&self) -> Box<dyn Iterator<Item = &EventTriggerConfig> + '_> {
        match self {
            EventTriggerConfigs::Single(config) => Box::new(std::iter::once(config)),
            EventTriggerConfigs::Multiple(configs) => Box::new(configs.iter()),
        }
    }

    /// Get all configurations as a slice-like iterator
    pub fn configs(&self) -> Vec<&EventTriggerConfig> {
        match self {
            EventTriggerConfigs::Single(config) => vec![config],
            EventTriggerConfigs::Multiple(configs) => configs.iter().collect(),
        }
    }

    /// Get the number of event configurations
    pub fn len(&self) -> usize {
        match self {
            EventTriggerConfigs::Single(_) => 1,
            EventTriggerConfigs::Multiple(configs) => configs.len(),
        }
    }

    /// Check if empty (only possible for Multiple with empty vec, which we prevent)
    pub fn is_empty(&self) -> bool {
        match self {
            EventTriggerConfigs::Single(_) => false,
            EventTriggerConfigs::Multiple(configs) => configs.is_empty(),
        }
    }
}

impl<'de> Deserialize<'de> for EventTriggerConfigs {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct EventTriggerConfigsVisitor;

        impl<'de> Visitor<'de> for EventTriggerConfigsVisitor {
            type Value = EventTriggerConfigs;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a single event trigger config object or an array of event trigger configs")
            }

            fn visit_map<M>(self, map: M) -> Result<EventTriggerConfigs, M::Error>
            where
                M: de::MapAccess<'de>,
            {
                // Deserialize as a single EventTriggerConfig
                let config = EventTriggerConfig::deserialize(de::value::MapAccessDeserializer::new(map))?;
                Ok(EventTriggerConfigs::Single(config))
            }

            fn visit_seq<S>(self, mut seq: S) -> Result<EventTriggerConfigs, S::Error>
            where
                S: de::SeqAccess<'de>,
            {
                let mut configs = Vec::new();
                while let Some(config) = seq.next_element::<EventTriggerConfig>()? {
                    configs.push(config);
                }
                if configs.is_empty() {
                    return Err(de::Error::custom("on_events array cannot be empty"));
                }
                Ok(EventTriggerConfigs::Multiple(configs))
            }
        }

        deserializer.deserialize_any(EventTriggerConfigsVisitor)
    }
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
    /// Call when specific events are emitted (supports single or multiple events)
    OnEvents(EventTriggerConfigs),
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

    pub fn as_on_events(&self) -> Option<&EventTriggerConfigs> {
        match self {
            Frequency::OnEvents(configs) => Some(configs),
            _ => None,
        }
    }

    /// Get all event trigger configurations (convenience method)
    pub fn event_configs(&self) -> Vec<&EventTriggerConfig> {
        match self {
            Frequency::OnEvents(configs) => configs.configs(),
            _ => Vec::new(),
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
                let mut on_events: Option<EventTriggerConfigs> = None;

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
                    Some(configs) => Ok(Frequency::OnEvents(configs)),
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
    /// Unnamed tuple: "(address, uint96)" - no field names
    UnnamedTuple(Vec<Box<EvmType>>),
    /// Dynamic array: "address[]" or "(address, uint96)[]"
    Array(Box<EvmType>),
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
    /// Handles: "uint256", "int256 latestAnswer", "(uint160 sqrtPriceX96, int24 tick)",
    /// "address[]", "(address, uint96)[]", "(address beneficiary, uint96 shares)[]"
    pub fn parse(s: &str) -> Result<EvmType, EvmTypeParseError> {
        let s = s.trim();

        // Check if it's an array (ends with [])
        if s.ends_with("[]") {
            let inner_str = &s[..s.len() - 2];
            let inner_type = Self::parse(inner_str)?;
            return Ok(EvmType::Array(Box::new(inner_type)));
        }

        // Check if it's a tuple
        if s.starts_with('(') && s.ends_with(')') {
            return Self::parse_tuple(s);
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

    /// Parse a tuple - detects whether named or unnamed based on field patterns
    /// Named: "(uint160 sqrtPriceX96, int24 tick)" - fields have "type name" pattern
    /// Unnamed: "(address, uint96)" - fields are just types
    fn parse_tuple(s: &str) -> Result<EvmType, EvmTypeParseError> {
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

        // First pass: determine if this is a named or unnamed tuple
        // A tuple is named if ALL non-tuple fields have "type name" pattern
        let is_named = Self::detect_named_tuple(&fields)?;

        if is_named {
            Self::parse_named_tuple_fields(&fields)
        } else {
            Self::parse_unnamed_tuple_fields(&fields)
        }
    }

    /// Detect if tuple fields follow named pattern (type + name) or unnamed pattern (type only)
    fn detect_named_tuple(fields: &[&str]) -> Result<bool, EvmTypeParseError> {
        for field in fields {
            let field = field.trim();
            if field.is_empty() {
                return Err(EvmTypeParseError::EmptyField);
            }

            if field.starts_with('(') {
                // Nested tuple/array - check if there's a name after the closing paren/bracket
                let (_, remainder) = Self::find_type_end(field)?;
                if !remainder.is_empty() {
                    // Has a name after type
                    return Ok(true);
                }
            } else {
                // Simple type - check if there's a space indicating a name
                if field.contains(' ') {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Find the end of a type expression (handles nested parens and array suffix)
    /// Returns (type_str, remainder after type)
    fn find_type_end(field: &str) -> Result<(&str, &str), EvmTypeParseError> {
        let field = field.trim();
        if field.starts_with('(') {
            // Find matching closing paren
            let mut depth = 0;
            let mut close_idx = None;
            for (i, c) in field.char_indices() {
                match c {
                    '(' => depth += 1,
                    ')' => {
                        depth -= 1;
                        if depth == 0 {
                            close_idx = Some(i);
                            break;
                        }
                    }
                    _ => {}
                }
            }
            let close_idx = close_idx.ok_or_else(|| {
                EvmTypeParseError::InvalidTuple(format!(
                    "unmatched parenthesis in field '{}'",
                    field
                ))
            })?;

            // Check for array suffix after the paren
            let after_paren = &field[close_idx + 1..];
            if after_paren.starts_with("[]") {
                let type_end = close_idx + 3; // include "[]"
                let remainder = field[type_end..].trim();
                Ok((&field[..type_end], remainder))
            } else {
                let remainder = field[close_idx + 1..].trim();
                Ok((&field[..=close_idx], remainder))
            }
        } else {
            // Simple type - find space or end
            if let Some(space_idx) = field.find(' ') {
                Ok((&field[..space_idx], field[space_idx..].trim()))
            } else {
                Ok((field, ""))
            }
        }
    }

    /// Parse fields as a named tuple
    fn parse_named_tuple_fields(fields: &[&str]) -> Result<EvmType, EvmTypeParseError> {
        let mut parsed_fields = Vec::new();
        for field in fields {
            let field = field.trim();
            if field.is_empty() {
                return Err(EvmTypeParseError::EmptyField);
            }

            let (type_str, name) = Self::find_type_end(field)?;
            if name.is_empty() {
                return Err(EvmTypeParseError::InvalidTuple(format!(
                    "field '{}' must have a name in named tuple",
                    field
                )));
            }

            let field_type = Self::parse(type_str)?;
            parsed_fields.push((name.to_string(), Box::new(field_type)));
        }
        Ok(EvmType::NamedTuple(parsed_fields))
    }

    /// Parse fields as an unnamed tuple
    fn parse_unnamed_tuple_fields(fields: &[&str]) -> Result<EvmType, EvmTypeParseError> {
        let mut parsed_fields = Vec::new();
        for field in fields {
            let field = field.trim();
            if field.is_empty() {
                return Err(EvmTypeParseError::EmptyField);
            }

            // Field should be just a type
            let field_type = Self::parse(field)?;
            parsed_fields.push(Box::new(field_type));
        }
        Ok(EvmType::UnnamedTuple(parsed_fields))
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

    /// Check if this is an unnamed tuple
    pub fn is_unnamed_tuple(&self) -> bool {
        matches!(self, EvmType::UnnamedTuple(_))
    }

    /// Check if this is an array
    pub fn is_array(&self) -> bool {
        matches!(self, EvmType::Array(_))
    }

    /// Get the element type for arrays
    pub fn array_element_type(&self) -> Option<&EvmType> {
        match self {
            EvmType::Array(inner) => Some(inner),
            _ => None,
        }
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
            // UnnamedTuple doesn't support parsing from ParamValue
            EvmType::UnnamedTuple(_) => Err(ParamError::TypeMismatch {
                expected: "simple type".to_string(),
                got: "unnamed tuple".to_string(),
            }),
            // Array doesn't support parsing from ParamValue
            EvmType::Array(_) => Err(ParamError::TypeMismatch {
                expected: "simple type".to_string(),
                got: "array".to_string(),
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
            // NamedTuple - struct with named fields
            EvmType::NamedTuple(fields) => {
                let struct_fields: Vec<Field> = fields
                    .iter()
                    .map(|(name, ty)| Field::new(name, ty.to_arrow_type(), true))
                    .collect();
                DataType::Struct(struct_fields.into())
            }
            // UnnamedTuple - struct with positional field names: "0", "1", etc.
            EvmType::UnnamedTuple(fields) => {
                let struct_fields: Vec<Field> = fields
                    .iter()
                    .enumerate()
                    .map(|(i, ty)| Field::new(i.to_string(), ty.to_arrow_type(), true))
                    .collect();
                DataType::Struct(struct_fields.into())
            }
            // Array - list of elements
            EvmType::Array(inner) => {
                DataType::List(Arc::new(Field::new("item", inner.to_arrow_type(), true)))
            }
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
        let configs = freq.event_configs();
        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].source, "Token");
        assert_eq!(configs[0].event, "Transfer(address,address,uint256)");
    }

    #[test]
    fn test_frequency_on_events_helpers() {
        let freq = Frequency::OnEvents(EventTriggerConfigs::Single(EventTriggerConfig {
            source: "Pool".to_string(),
            event: "Swap(address,address,int256,int256,uint160,uint128,int24)".to_string(),
        }));
        assert!(freq.is_on_events());
        assert!(!freq.is_once());
        assert!(freq.as_on_events().is_some());
        assert_eq!(freq.event_configs().len(), 1);
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
        let configs = config.frequency.event_configs();
        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].source, "V3Pool");
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

    #[test]
    fn test_frequency_deserialize_on_events_array() {
        let json = r#"{"on_events": [
            {"source": "V3Pool", "event": "Swap(address,address,int256,int256,uint160,uint128,int24)"},
            {"source": "V3Pool", "event": "Mint(address,int24,int24,uint128,uint256,uint256)"},
            {"source": "V3Pool", "event": "Burn(address,int24,int24,uint128,uint256,uint256)"}
        ]}"#;
        let freq: Frequency = serde_json::from_str(json).unwrap();
        assert!(freq.is_on_events());
        let configs = freq.event_configs();
        assert_eq!(configs.len(), 3);
        assert_eq!(configs[0].source, "V3Pool");
        assert_eq!(configs[0].event, "Swap(address,address,int256,int256,uint160,uint128,int24)");
        assert_eq!(configs[1].event, "Mint(address,int24,int24,uint128,uint256,uint256)");
        assert_eq!(configs[2].event, "Burn(address,int24,int24,uint128,uint256,uint256)");
    }

    #[test]
    fn test_frequency_deserialize_on_events_single_still_works() {
        // Backward compatibility - single object syntax still works
        let json = r#"{"on_events": {"source": "Token", "event": "Transfer(address,address,uint256)"}}"#;
        let freq: Frequency = serde_json::from_str(json).unwrap();
        assert!(freq.is_on_events());
        let configs = freq.event_configs();
        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].source, "Token");
    }

    #[test]
    fn test_frequency_deserialize_on_events_empty_array_fails() {
        let json = r#"{"on_events": []}"#;
        let result: Result<Frequency, _> = serde_json::from_str(json);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("empty"), "Expected error about empty array, got: {}", err);
    }

    #[test]
    fn test_eth_call_config_with_multiple_events() {
        let json = r#"{
            "function": "slot0()",
            "output_type": "(uint160 sqrtPriceX96, int24 tick)",
            "frequency": {
                "on_events": [
                    {"source": "V3Pool", "event": "Swap(address,address,int256,int256,uint160,uint128,int24)"},
                    {"source": "V3Pool", "event": "Mint(address,int24,int24,uint128,uint256,uint256)"}
                ]
            }
        }"#;
        let config: EthCallConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.function, "slot0()");
        assert!(config.frequency.is_on_events());
        let trigger_configs = config.frequency.event_configs();
        assert_eq!(trigger_configs.len(), 2);
        assert!(trigger_configs[0].event.contains("Swap"));
        assert!(trigger_configs[1].event.contains("Mint"));
    }

    #[test]
    fn test_event_trigger_configs_iter() {
        // Test Single variant
        let single = EventTriggerConfigs::Single(EventTriggerConfig {
            source: "Token".to_string(),
            event: "Transfer(address,address,uint256)".to_string(),
        });
        let configs: Vec<_> = single.iter().collect();
        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].source, "Token");

        // Test Multiple variant
        let multiple = EventTriggerConfigs::Multiple(vec![
            EventTriggerConfig {
                source: "Pool".to_string(),
                event: "Swap(...)".to_string(),
            },
            EventTriggerConfig {
                source: "Pool".to_string(),
                event: "Mint(...)".to_string(),
            },
        ]);
        let configs: Vec<_> = multiple.iter().collect();
        assert_eq!(configs.len(), 2);
        assert_eq!(configs[0].event, "Swap(...)");
        assert_eq!(configs[1].event, "Mint(...)");
    }

    #[test]
    fn test_event_trigger_configs_len_and_is_empty() {
        let single = EventTriggerConfigs::Single(EventTriggerConfig {
            source: "Token".to_string(),
            event: "Transfer(...)".to_string(),
        });
        assert_eq!(single.len(), 1);
        assert!(!single.is_empty());

        let multiple = EventTriggerConfigs::Multiple(vec![
            EventTriggerConfig {
                source: "Pool".to_string(),
                event: "Swap(...)".to_string(),
            },
            EventTriggerConfig {
                source: "Pool".to_string(),
                event: "Mint(...)".to_string(),
            },
        ]);
        assert_eq!(multiple.len(), 2);
        assert!(!multiple.is_empty());
    }

    #[test]
    fn test_param_config_from_event_address() {
        // Test that from_event: "address" is parsed correctly
        let json = r#"{"type": "address", "from_event": "address"}"#;
        let param: ParamConfig = serde_json::from_str(json).unwrap();
        assert_eq!(*param.param_type(), EvmType::Address);
        assert_eq!(param.from_event(), Some("address"));
        assert!(!param.is_self_address());
    }

    // Array type parsing tests

    #[test]
    fn test_parse_simple_array() {
        let parsed = EvmType::parse("address[]").unwrap();
        assert!(matches!(parsed, EvmType::Array(_)));
        if let EvmType::Array(inner) = parsed {
            assert_eq!(*inner, EvmType::Address);
        }
    }

    #[test]
    fn test_parse_uint256_array() {
        let parsed = EvmType::parse("uint256[]").unwrap();
        assert!(matches!(parsed, EvmType::Array(_)));
        if let EvmType::Array(inner) = parsed {
            assert_eq!(*inner, EvmType::Uint256);
        }
    }

    #[test]
    fn test_parse_unnamed_tuple() {
        let parsed = EvmType::parse("(address, uint96)").unwrap();
        assert!(matches!(parsed, EvmType::UnnamedTuple(_)));
        if let EvmType::UnnamedTuple(fields) = parsed {
            assert_eq!(fields.len(), 2);
            assert_eq!(*fields[0], EvmType::Address);
            assert_eq!(*fields[1], EvmType::Uint96);
        }
    }

    #[test]
    fn test_parse_array_of_unnamed_tuple() {
        let parsed = EvmType::parse("(address, uint96)[]").unwrap();
        assert!(matches!(parsed, EvmType::Array(_)));
        if let EvmType::Array(inner) = &parsed {
            assert!(matches!(inner.as_ref(), EvmType::UnnamedTuple(_)));
            if let EvmType::UnnamedTuple(fields) = inner.as_ref() {
                assert_eq!(fields.len(), 2);
                assert_eq!(*fields[0], EvmType::Address);
                assert_eq!(*fields[1], EvmType::Uint96);
            }
        }
    }

    #[test]
    fn test_parse_array_of_named_tuple() {
        let parsed = EvmType::parse("(address beneficiary, uint96 shares)[]").unwrap();
        assert!(matches!(parsed, EvmType::Array(_)));
        if let EvmType::Array(inner) = &parsed {
            assert!(matches!(inner.as_ref(), EvmType::NamedTuple(_)));
            if let EvmType::NamedTuple(fields) = inner.as_ref() {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].0, "beneficiary");
                assert_eq!(*fields[0].1, EvmType::Address);
                assert_eq!(fields[1].0, "shares");
                assert_eq!(*fields[1].1, EvmType::Uint96);
            }
        }
    }

    #[test]
    fn test_parse_named_tuple_with_array_field() {
        let parsed = EvmType::parse("((address, uint96)[] beneficiaryData)").unwrap();
        assert!(matches!(parsed, EvmType::NamedTuple(_)));
        if let EvmType::NamedTuple(fields) = parsed {
            assert_eq!(fields.len(), 1);
            assert_eq!(fields[0].0, "beneficiaryData");
            assert!(matches!(fields[0].1.as_ref(), EvmType::Array(_)));
        }
    }

    #[test]
    fn test_parse_named_tuple_unchanged() {
        // Verify existing named tuple parsing still works
        let parsed = EvmType::parse("(uint160 sqrtPriceX96, int24 tick)").unwrap();
        assert!(matches!(parsed, EvmType::NamedTuple(_)));
        if let EvmType::NamedTuple(fields) = parsed {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].0, "sqrtPriceX96");
            assert_eq!(*fields[0].1, EvmType::Uint160);
            assert_eq!(fields[1].0, "tick");
            assert_eq!(*fields[1].1, EvmType::Int24);
        }
    }

    #[test]
    fn test_evm_type_is_array() {
        let arr = EvmType::parse("address[]").unwrap();
        assert!(arr.is_array());
        assert!(!arr.is_unnamed_tuple());
        assert!(!arr.is_named_tuple());
    }

    #[test]
    fn test_evm_type_is_unnamed_tuple() {
        let tuple = EvmType::parse("(address, uint96)").unwrap();
        assert!(tuple.is_unnamed_tuple());
        assert!(!tuple.is_array());
        assert!(!tuple.is_named_tuple());
    }

    #[test]
    fn test_array_element_type() {
        let arr = EvmType::parse("address[]").unwrap();
        let elem = arr.array_element_type().unwrap();
        assert_eq!(*elem, EvmType::Address);
    }

    #[test]
    fn test_parse_nested_named_tuple() {
        // Like getState output: (address numeraire, uint8 status, (address currency0, ...) poolKey, int24 farTick)
        let parsed = EvmType::parse("(address numeraire, uint8 status, (address currency0, address currency1, uint24 fee) poolKey, int24 farTick)").unwrap();
        assert!(matches!(parsed, EvmType::NamedTuple(_)));
        if let EvmType::NamedTuple(fields) = &parsed {
            assert_eq!(fields.len(), 4);
            assert_eq!(fields[0].0, "numeraire");
            assert_eq!(*fields[0].1, EvmType::Address);
            assert_eq!(fields[1].0, "status");
            assert_eq!(*fields[1].1, EvmType::Uint8);
            assert_eq!(fields[2].0, "poolKey");
            // poolKey should be a nested NamedTuple
            assert!(matches!(fields[2].1.as_ref(), EvmType::NamedTuple(_)));
            if let EvmType::NamedTuple(pool_fields) = fields[2].1.as_ref() {
                assert_eq!(pool_fields.len(), 3);
                assert_eq!(pool_fields[0].0, "currency0");
                assert_eq!(pool_fields[1].0, "currency1");
                assert_eq!(pool_fields[2].0, "fee");
            }
            assert_eq!(fields[3].0, "farTick");
            assert_eq!(*fields[3].1, EvmType::Int24);
        }
    }
}
