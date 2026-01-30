use alloy::dyn_abi::DynSolType;
use alloy::primitives::keccak256;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum EventParseError {
    #[error("Invalid event signature: {0}")]
    InvalidSignature(String),
    #[error("Failed to parse type '{0}': {1}")]
    TypeParse(String, String),
    #[error("Missing closing parenthesis")]
    MissingCloseParen,
}

/// Parsed event parameter
#[derive(Debug, Clone)]
pub struct EventParam {
    pub name: String,
    pub param_type: DynSolType,
    pub type_string: String,
    pub indexed: bool,
}

/// Parsed event definition
#[derive(Debug, Clone)]
pub struct ParsedEvent {
    pub name: String,
    pub signature: String,
    pub canonical_signature: String,
    pub topic0: [u8; 32],
    pub params: Vec<EventParam>,
}

impl ParsedEvent {
    /// Parse a full ABI signature like "Transfer(address indexed from, address indexed to, uint256 value)"
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

        // Find matching closing paren
        let close_paren = signature
            .rfind(')')
            .ok_or(EventParseError::MissingCloseParen)?;

        let params_str = &signature[open_paren + 1..close_paren];

        // Parse parameters
        let params = parse_params(params_str)?;

        // Build canonical signature (types only, no names, no "indexed")
        let type_strings: Vec<&str> = params.iter().map(|p| p.type_string.as_str()).collect();
        let canonical_signature = format!("{}({})", name, type_strings.join(","));

        // Compute topic0
        let topic0 = keccak256(canonical_signature.as_bytes()).0;

        Ok(ParsedEvent {
            name,
            signature: signature.to_string(),
            canonical_signature,
            topic0,
            params,
        })
    }

    /// Get indexed parameters
    pub fn indexed_params(&self) -> Vec<&EventParam> {
        self.params.iter().filter(|p| p.indexed).collect()
    }

    /// Get non-indexed (data) parameters
    pub fn data_params(&self) -> Vec<&EventParam> {
        self.params.iter().filter(|p| !p.indexed).collect()
    }
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
fn parse_single_param(param_str: &str) -> Result<EventParam, EventParseError> {
    let parts: Vec<&str> = param_str.split_whitespace().collect();

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
    })
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
}
