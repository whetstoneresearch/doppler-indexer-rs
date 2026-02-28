use alloy::dyn_abi::DynSolType;
use alloy::primitives::keccak256;
use arrow::datatypes::DataType;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum EventParseError {
    #[error("Invalid event signature: {0}")]
    InvalidSignature(String),
    #[error("Failed to parse type '{0}': {1}")]
    TypeParse(String, String),
    #[error("Missing closing parenthesis")]
    MissingCloseParen,
    #[error("Invalid tuple format: {0}")]
    InvalidTuple(String),
    #[error("Empty tuple field")]
    EmptyField,
}

/// Information about tuple field names (supports nesting)
#[derive(Debug, Clone)]
pub enum TupleFieldInfo {
    /// A leaf field (not a tuple)
    Leaf,
    /// A tuple with named fields: Vec<(field_name, field_info)>
    Tuple(Vec<(String, TupleFieldInfo)>),
}

/// A flattened field for parquet output
#[derive(Debug, Clone)]
pub struct FlattenedField {
    /// Full path name like "key.currency0", "deployer", or "poolKey.hash"
    pub full_name: String,
    /// The leaf type for this field (FixedBytes(32) for indexed tuple hashes)
    pub leaf_type: DynSolType,
    /// Arrow data type for parquet writing
    pub arrow_type: DataType,
    /// Whether this field comes from an indexed parameter
    pub from_indexed: bool,
    /// True if this is a hash of an indexed tuple (cannot be decoded)
    pub is_indexed_tuple_hash: bool,
}

/// Parsed event parameter
#[derive(Debug, Clone)]
pub struct EventParam {
    pub name: String,
    pub param_type: DynSolType,
    pub type_string: String,
    pub indexed: bool,
    /// Field names within this parameter (for tuples)
    /// For non-tuple params, this is None or Leaf
    /// For tuples, contains field names in order (can be nested)
    pub tuple_fields: Option<TupleFieldInfo>,
}

/// Parsed event definition
#[derive(Debug, Clone)]
pub struct ParsedEvent {
    pub name: String,
    pub signature: String,
    pub canonical_signature: String,
    pub topic0: [u8; 32],
    pub params: Vec<EventParam>,
    /// Flattened fields for parquet output (derived from params)
    pub flattened_fields: Vec<FlattenedField>,
}

impl ParsedEvent {
    /// Parse a full ABI signature like "Transfer(address indexed from, address indexed to, uint256 value)"
    /// Also supports named tuples like "Swap((address currency0, address currency1) key, uint256 amount)"
    pub fn from_signature(signature: &str) -> Result<Self, EventParseError> {
        let signature = signature.trim();

        // Find the event name (everything before first '(')
        let open_paren = signature
            .find('(')
            .ok_or_else(|| EventParseError::InvalidSignature(signature.to_string()))?;

        let name = signature[..open_paren].trim().to_string();
        if name.is_empty() {
            return Err(EventParseError::InvalidSignature(
                "Empty event name".to_string(),
            ));
        }

        // Find matching closing paren for the event signature
        let close_paren = find_matching_close_paren(signature, open_paren)
            .ok_or(EventParseError::MissingCloseParen)?;

        let params_str = &signature[open_paren + 1..close_paren];

        // Parse parameters
        let params = parse_params(params_str)?;

        // Build canonical signature (types only, no names, no "indexed")
        let type_strings: Vec<&str> = params.iter().map(|p| p.type_string.as_str()).collect();
        let canonical_signature = format!("{}({})", name, type_strings.join(","));

        // Compute topic0
        let topic0 = keccak256(canonical_signature.as_bytes()).0;

        let mut event = ParsedEvent {
            name,
            signature: signature.to_string(),
            canonical_signature,
            topic0,
            params,
            flattened_fields: Vec::new(),
        };

        // Compute flattened fields for parquet output
        event.compute_flattened_fields();

        Ok(event)
    }

    /// Get indexed parameters
    pub fn indexed_params(&self) -> Vec<&EventParam> {
        self.params.iter().filter(|p| p.indexed).collect()
    }

    /// Get non-indexed (data) parameters
    pub fn data_params(&self) -> Vec<&EventParam> {
        self.params.iter().filter(|p| !p.indexed).collect()
    }

    /// Compute flattened fields from params for parquet output
    fn compute_flattened_fields(&mut self) {
        self.flattened_fields.clear();

        // Clone params to avoid borrowing conflict
        let params: Vec<(usize, EventParam)> = self
            .params
            .iter()
            .enumerate()
            .map(|(i, p)| (i, p.clone()))
            .collect();

        for (idx, param) in params {
            let prefix = if param.name.is_empty() {
                format!("param_{}", idx)
            } else {
                param.name.clone()
            };

            flatten_param_into(&param, &prefix, &mut self.flattened_fields);
        }
    }
}

/// Recursively flatten a parameter into FlattenedFields
fn flatten_param_into(param: &EventParam, prefix: &str, output: &mut Vec<FlattenedField>) {
    // Check if this is an indexed tuple - can't decode, store as hash
    if param.indexed && param.tuple_fields.is_some() {
        if let Some(TupleFieldInfo::Tuple(_)) = &param.tuple_fields {
            output.push(FlattenedField {
                full_name: format!("{}.hash", prefix),
                leaf_type: DynSolType::FixedBytes(32),
                arrow_type: DataType::FixedSizeBinary(32),
                from_indexed: true,
                is_indexed_tuple_hash: true,
            });
            return;
        }
    }

    match &param.tuple_fields {
        None | Some(TupleFieldInfo::Leaf) => {
            // Leaf field - add directly
            output.push(FlattenedField {
                full_name: prefix.to_string(),
                leaf_type: param.param_type.clone(),
                arrow_type: param_type_to_arrow(&param.param_type),
                from_indexed: param.indexed,
                is_indexed_tuple_hash: false,
            });
        }
        Some(TupleFieldInfo::Tuple(fields)) => {
            // Tuple - get child types and recurse
            if let DynSolType::Tuple(types) = &param.param_type {
                for ((field_name, field_info), field_type) in fields.iter().zip(types.iter()) {
                    let new_prefix = format!("{}.{}", prefix, field_name);

                    // Create a synthetic EventParam for recursion
                    let sub_param = EventParam {
                        name: field_name.clone(),
                        param_type: field_type.clone(),
                        type_string: String::new(), // Not needed for flattening
                        indexed: param.indexed,
                        tuple_fields: Some(field_info.clone()),
                    };
                    flatten_param_into(&sub_param, &new_prefix, output);
                }
            }
        }
    }
}

/// Find the matching close paren for an open paren at the given position
fn find_matching_close_paren(s: &str, open_pos: usize) -> Option<usize> {
    let mut depth = 0;
    for (i, c) in s[open_pos..].char_indices() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(open_pos + i);
                }
            }
            _ => {}
        }
    }
    None
}

/// Parse the parameter list from an event signature
fn parse_params(params_str: &str) -> Result<Vec<EventParam>, EventParseError> {
    let params_str = params_str.trim();
    if params_str.is_empty() {
        return Ok(Vec::new());
    }

    let mut params = Vec::new();
    let mut current = String::new();
    let mut paren_depth = 0;

    // Split by comma, respecting nested parentheses (for tuple types)
    for c in params_str.chars() {
        match c {
            '(' => {
                paren_depth += 1;
                current.push(c);
            }
            ')' => {
                paren_depth -= 1;
                current.push(c);
            }
            ',' if paren_depth == 0 => {
                if !current.trim().is_empty() {
                    params.push(parse_single_param(current.trim())?);
                }
                current.clear();
            }
            _ => {
                current.push(c);
            }
        }
    }

    // Don't forget the last parameter
    if !current.trim().is_empty() {
        params.push(parse_single_param(current.trim())?);
    }

    Ok(params)
}

/// Parse a single parameter like "address indexed from" or "uint256 value"
/// Also handles tuple params like "(address currency0, address currency1) key"
fn parse_single_param(param_str: &str) -> Result<EventParam, EventParseError> {
    let trimmed = param_str.trim();

    // Check if this is a tuple parameter
    if trimmed.starts_with('(') {
        return parse_tuple_param(trimmed);
    }

    // Simple parameter parsing
    let parts: Vec<&str> = trimmed.split_whitespace().collect();

    if parts.is_empty() {
        return Err(EventParseError::InvalidSignature(
            "Empty parameter".to_string(),
        ));
    }

    // Determine if indexed and extract type and name
    let (type_string, indexed, name) = if parts.len() == 1 {
        // Just type, no name
        (parts[0].to_string(), false, String::new())
    } else if parts.len() == 2 {
        if parts[1] == "indexed" {
            // "type indexed" - no name
            (parts[0].to_string(), true, String::new())
        } else {
            // "type name"
            (parts[0].to_string(), false, parts[1].to_string())
        }
    } else if parts.len() == 3 {
        if parts[1] == "indexed" {
            // "type indexed name"
            (parts[0].to_string(), true, parts[2].to_string())
        } else {
            return Err(EventParseError::InvalidSignature(format!(
                "Invalid parameter format: {}",
                param_str
            )));
        }
    } else {
        return Err(EventParseError::InvalidSignature(format!(
            "Too many parts in parameter: {}",
            param_str
        )));
    };

    // Parse the type string into DynSolType
    let param_type = DynSolType::parse(&type_string)
        .map_err(|e| EventParseError::TypeParse(type_string.clone(), e.to_string()))?;

    Ok(EventParam {
        name,
        param_type,
        type_string,
        indexed,
        tuple_fields: None,
    })
}

/// Parse a tuple parameter like "(address currency0, address currency1, uint24 fee) key"
/// or "(address currency0, address currency1) indexed poolKey"
fn parse_tuple_param(param_str: &str) -> Result<EventParam, EventParseError> {
    // Find matching close paren
    let close_idx = find_matching_close_paren(param_str, 0)
        .ok_or_else(|| EventParseError::InvalidTuple(param_str.to_string()))?;

    let tuple_content = &param_str[1..close_idx];
    let after_paren = param_str[close_idx + 1..].trim();

    // Parse what comes after ): could be "indexed ParamName", "ParamName", "indexed", or empty
    let (indexed, name) = parse_indexed_and_name(after_paren);

    // Parse tuple fields (recursively for nested tuples)
    let (field_info, canonical_types) = parse_tuple_fields(tuple_content)?;

    // Build canonical type string (no names)
    let type_string = format!("({})", canonical_types.join(","));

    // Parse into DynSolType
    let param_type = DynSolType::parse(&type_string)
        .map_err(|e| EventParseError::TypeParse(type_string.clone(), e.to_string()))?;

    Ok(EventParam {
        name,
        param_type,
        type_string,
        indexed,
        tuple_fields: Some(TupleFieldInfo::Tuple(field_info)),
    })
}

/// Parse "indexed ParamName", "ParamName", "indexed", or empty string
/// Returns (indexed, name)
fn parse_indexed_and_name(s: &str) -> (bool, String) {
    let parts: Vec<&str> = s.split_whitespace().collect();

    match parts.len() {
        0 => (false, String::new()),
        1 => {
            if parts[0] == "indexed" {
                (true, String::new())
            } else {
                (false, parts[0].to_string())
            }
        }
        2 => {
            if parts[0] == "indexed" {
                (true, parts[1].to_string())
            } else {
                // Shouldn't happen, but treat first as name
                (false, parts[0].to_string())
            }
        }
        _ => {
            // Take first two parts
            if parts[0] == "indexed" {
                (true, parts[1].to_string())
            } else {
                (false, parts[0].to_string())
            }
        }
    }
}

/// Parse tuple fields content (the part inside parentheses)
/// Returns (field_info, canonical_type_strings)
fn parse_tuple_fields(
    content: &str,
) -> Result<(Vec<(String, TupleFieldInfo)>, Vec<String>), EventParseError> {
    let content = content.trim();
    if content.is_empty() {
        return Err(EventParseError::InvalidTuple("empty tuple".to_string()));
    }

    // Split by comma, respecting nested parentheses
    let fields = split_tuple_fields(content);

    let mut field_info = Vec::new();
    let mut canonical_types = Vec::new();

    for field in fields {
        let field = field.trim();
        if field.is_empty() {
            return Err(EventParseError::EmptyField);
        }

        // Check if this field is itself a tuple
        if field.starts_with('(') {
            // Find matching close paren
            let close_idx = find_matching_close_paren(field, 0)
                .ok_or_else(|| EventParseError::InvalidTuple(field.to_string()))?;

            let nested_content = &field[1..close_idx];
            let after_nested = field[close_idx + 1..].trim();

            // Get the field name (should be the last word after the tuple)
            let name = after_nested
                .split_whitespace()
                .last()
                .ok_or_else(|| {
                    EventParseError::InvalidTuple(format!("tuple field '{}' has no name", field))
                })?
                .to_string();

            // Recursively parse nested tuple
            let (nested_info, nested_types) = parse_tuple_fields(nested_content)?;
            let nested_type_string = format!("({})", nested_types.join(","));

            field_info.push((name, TupleFieldInfo::Tuple(nested_info)));
            canonical_types.push(nested_type_string);
        } else {
            // Simple field: "type name" or just "type"
            let parts: Vec<&str> = field.splitn(2, ' ').collect();

            let (type_str, name) = if parts.len() == 2 {
                (parts[0].trim(), parts[1].trim().to_string())
            } else {
                // No name - generate one
                (parts[0].trim(), String::new())
            };

            if name.is_empty() {
                return Err(EventParseError::InvalidTuple(format!(
                    "field '{}' must have a name in named tuple",
                    field
                )));
            }

            // Validate the type by parsing it
            DynSolType::parse(type_str)
                .map_err(|e| EventParseError::TypeParse(type_str.to_string(), e.to_string()))?;

            field_info.push((name, TupleFieldInfo::Leaf));
            canonical_types.push(type_str.to_string());
        }
    }

    Ok((field_info, canonical_types))
}

/// Split tuple fields by comma, respecting nested parentheses
fn split_tuple_fields(s: &str) -> Vec<&str> {
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

    fields
}

/// Convert param type to Arrow DataType for parquet writing
pub fn param_type_to_arrow(param_type: &DynSolType) -> DataType {
    match param_type {
        DynSolType::Address => DataType::FixedSizeBinary(20),
        DynSolType::Uint(bits) => {
            if *bits <= 8 {
                DataType::UInt8
            } else if *bits <= 64 {
                DataType::UInt64
            } else {
                DataType::Utf8 // Large uints as strings
            }
        }
        DynSolType::Int(bits) => {
            if *bits <= 8 {
                DataType::Int8
            } else if *bits <= 64 {
                DataType::Int64
            } else {
                DataType::Utf8 // Large ints as strings
            }
        }
        DynSolType::Bool => DataType::Boolean,
        DynSolType::FixedBytes(32) => DataType::FixedSizeBinary(32),
        DynSolType::FixedBytes(_) => DataType::Binary,
        DynSolType::Bytes => DataType::Binary,
        DynSolType::String => DataType::Utf8,
        DynSolType::Tuple(_) => DataType::Binary, // Fallback for complex types
        DynSolType::Array(_) => DataType::Binary,
        DynSolType::FixedArray(_, _) => DataType::Binary,
        _ => DataType::Binary, // Fallback
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_transfer_event() {
        let sig = "Transfer(address indexed from, address indexed to, uint256 value)";
        let parsed = ParsedEvent::from_signature(sig).unwrap();

        assert_eq!(parsed.name, "Transfer");
        assert_eq!(
            parsed.canonical_signature,
            "Transfer(address,address,uint256)"
        );
        assert_eq!(parsed.params.len(), 3);

        assert_eq!(parsed.params[0].name, "from");
        assert!(parsed.params[0].indexed);
        assert_eq!(parsed.params[0].type_string, "address");

        assert_eq!(parsed.params[1].name, "to");
        assert!(parsed.params[1].indexed);

        assert_eq!(parsed.params[2].name, "value");
        assert!(!parsed.params[2].indexed);
        assert_eq!(parsed.params[2].type_string, "uint256");

        assert_eq!(parsed.indexed_params().len(), 2);
        assert_eq!(parsed.data_params().len(), 1);

        // Check flattened fields
        assert_eq!(parsed.flattened_fields.len(), 3);
        assert_eq!(parsed.flattened_fields[0].full_name, "from");
        assert_eq!(parsed.flattened_fields[1].full_name, "to");
        assert_eq!(parsed.flattened_fields[2].full_name, "value");
    }

    #[test]
    fn test_parse_swap_event() {
        let sig = "Swap(address indexed sender, uint256 amount0In, uint256 amount1In, uint256 amount0Out, uint256 amount1Out, address indexed to)";
        let parsed = ParsedEvent::from_signature(sig).unwrap();

        assert_eq!(parsed.name, "Swap");
        assert_eq!(parsed.params.len(), 6);
        assert_eq!(parsed.indexed_params().len(), 2);
        assert_eq!(parsed.data_params().len(), 4);
    }

    #[test]
    fn test_parse_empty_params() {
        let sig = "Paused()";
        let parsed = ParsedEvent::from_signature(sig).unwrap();

        assert_eq!(parsed.name, "Paused");
        assert_eq!(parsed.params.len(), 0);
        assert_eq!(parsed.canonical_signature, "Paused()");
        assert_eq!(parsed.flattened_fields.len(), 0);
    }

    #[test]
    fn test_topic0_computation() {
        // Transfer(address,address,uint256) has a well-known topic0
        let sig = "Transfer(address indexed from, address indexed to, uint256 value)";
        let parsed = ParsedEvent::from_signature(sig).unwrap();

        // Known keccak256 of "Transfer(address,address,uint256)"
        let expected_topic0 =
            hex::decode("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
                .unwrap();
        assert_eq!(parsed.topic0.as_slice(), expected_topic0.as_slice());
    }

    #[test]
    fn test_parse_named_tuple() {
        let sig = "ModifyLiquidity((address currency0, address currency1, uint24 fee, int24 tickSpacing, address hooks) key, (int24 tickLower, int24 tickUpper, int256 liquidityDelta, bytes32 salt) params)";
        let parsed = ParsedEvent::from_signature(sig).unwrap();

        assert_eq!(parsed.name, "ModifyLiquidity");
        assert_eq!(parsed.params.len(), 2);

        // Check first tuple param
        assert_eq!(parsed.params[0].name, "key");
        assert!(!parsed.params[0].indexed);
        assert_eq!(
            parsed.params[0].type_string,
            "(address,address,uint24,int24,address)"
        );
        assert!(matches!(
            parsed.params[0].tuple_fields,
            Some(TupleFieldInfo::Tuple(_))
        ));

        // Check second tuple param
        assert_eq!(parsed.params[1].name, "params");
        assert_eq!(
            parsed.params[1].type_string,
            "(int24,int24,int256,bytes32)"
        );

        // Check canonical signature
        assert_eq!(
            parsed.canonical_signature,
            "ModifyLiquidity((address,address,uint24,int24,address),(int24,int24,int256,bytes32))"
        );

        // Check flattened fields
        assert_eq!(parsed.flattened_fields.len(), 9);
        assert_eq!(parsed.flattened_fields[0].full_name, "key.currency0");
        assert_eq!(parsed.flattened_fields[1].full_name, "key.currency1");
        assert_eq!(parsed.flattened_fields[2].full_name, "key.fee");
        assert_eq!(parsed.flattened_fields[3].full_name, "key.tickSpacing");
        assert_eq!(parsed.flattened_fields[4].full_name, "key.hooks");
        assert_eq!(parsed.flattened_fields[5].full_name, "params.tickLower");
        assert_eq!(parsed.flattened_fields[6].full_name, "params.tickUpper");
        assert_eq!(parsed.flattened_fields[7].full_name, "params.liquidityDelta");
        assert_eq!(parsed.flattened_fields[8].full_name, "params.salt");
    }

    #[test]
    fn test_parse_indexed_tuple() {
        let sig = "Swap(address indexed sender, (address currency0, address currency1, uint24 fee) indexed poolKey, bytes32 indexed poolId)";
        let parsed = ParsedEvent::from_signature(sig).unwrap();

        assert_eq!(parsed.name, "Swap");
        assert_eq!(parsed.params.len(), 3);

        // poolKey is indexed tuple
        assert_eq!(parsed.params[1].name, "poolKey");
        assert!(parsed.params[1].indexed);
        assert!(matches!(
            parsed.params[1].tuple_fields,
            Some(TupleFieldInfo::Tuple(_))
        ));

        // Check flattened fields - indexed tuple should be a hash
        assert_eq!(parsed.flattened_fields.len(), 3);
        assert_eq!(parsed.flattened_fields[0].full_name, "sender");
        assert!(!parsed.flattened_fields[0].is_indexed_tuple_hash);

        assert_eq!(parsed.flattened_fields[1].full_name, "poolKey.hash");
        assert!(parsed.flattened_fields[1].is_indexed_tuple_hash);
        assert!(parsed.flattened_fields[1].from_indexed);

        assert_eq!(parsed.flattened_fields[2].full_name, "poolId");
    }

    #[test]
    fn test_parse_mixed_params() {
        let sig = "Test((address a, uint256 b) key, address deployer)";
        let parsed = ParsedEvent::from_signature(sig).unwrap();

        assert_eq!(parsed.name, "Test");
        assert_eq!(parsed.params.len(), 2);
        assert_eq!(parsed.canonical_signature, "Test((address,uint256),address)");

        // Check flattened fields
        assert_eq!(parsed.flattened_fields.len(), 3);
        assert_eq!(parsed.flattened_fields[0].full_name, "key.a");
        assert_eq!(parsed.flattened_fields[1].full_name, "key.b");
        assert_eq!(parsed.flattened_fields[2].full_name, "deployer");
    }

    #[test]
    fn test_parse_complex_multicurve_swap() {
        let sig = "Swap(address indexed sender, (address currency0, address currency1, uint24 fee, int24 tickSpacing, address hooks) indexed poolKey, bytes32 indexed poolId, (bool zeroForOne, int256 amountSpecified, uint160 sqrtPriceLimitX96) params, int128 amount0, int128 amount1, bytes hookData)";
        let parsed = ParsedEvent::from_signature(sig).unwrap();

        assert_eq!(parsed.name, "Swap");
        assert_eq!(parsed.params.len(), 7);

        // Check canonical signature
        assert_eq!(
            parsed.canonical_signature,
            "Swap(address,(address,address,uint24,int24,address),bytes32,(bool,int256,uint160),int128,int128,bytes)"
        );

        // Check flattened fields
        // sender, poolKey.hash, poolId, params.zeroForOne, params.amountSpecified, params.sqrtPriceLimitX96, amount0, amount1, hookData
        assert_eq!(parsed.flattened_fields.len(), 9);
        assert_eq!(parsed.flattened_fields[0].full_name, "sender");
        assert_eq!(parsed.flattened_fields[1].full_name, "poolKey.hash");
        assert!(parsed.flattened_fields[1].is_indexed_tuple_hash);
        assert_eq!(parsed.flattened_fields[2].full_name, "poolId");
        assert_eq!(parsed.flattened_fields[3].full_name, "params.zeroForOne");
        assert_eq!(parsed.flattened_fields[4].full_name, "params.amountSpecified");
        assert_eq!(parsed.flattened_fields[5].full_name, "params.sqrtPriceLimitX96");
        assert_eq!(parsed.flattened_fields[6].full_name, "amount0");
        assert_eq!(parsed.flattened_fields[7].full_name, "amount1");
        assert_eq!(parsed.flattened_fields[8].full_name, "hookData");
    }

    #[test]
    fn test_parse_v4_pool_manager_events() {
        // From config/contracts/base/v4.json
        let swap_sig = "Swap(bytes32 indexed id, address indexed sender, int128 amount0, int128 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick, uint24 fee)";
        let parsed = ParsedEvent::from_signature(swap_sig).unwrap();

        assert_eq!(parsed.name, "Swap");
        assert_eq!(parsed.flattened_fields.len(), 8);
        assert_eq!(parsed.flattened_fields[0].full_name, "id");
        assert_eq!(parsed.flattened_fields[1].full_name, "sender");
        assert_eq!(parsed.flattened_fields[2].full_name, "amount0");
        assert_eq!(parsed.flattened_fields[3].full_name, "amount1");
        assert_eq!(parsed.flattened_fields[4].full_name, "sqrtPriceX96");
        assert_eq!(parsed.flattened_fields[5].full_name, "liquidity");
        assert_eq!(parsed.flattened_fields[6].full_name, "tick");
        assert_eq!(parsed.flattened_fields[7].full_name, "fee");
    }
}
