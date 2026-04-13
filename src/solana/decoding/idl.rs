//! Anchor IDL parser and discriminator-indexed decoder.
//!
//! Parses both pre-0.30 and 0.30+ IDL JSON formats, builds discriminator lookup
//! maps, and provides the `AnchorDecoder` which implements `ProgramDecoder`.

use std::collections::HashMap;

use solana_sdk::hash::hash as sha256;

use super::borsh_dynamic;
use super::traits::{
    DecodedAccountFields, DecodedEventFields, DecodedInstructionFields, ProgramDecoder,
    SolanaDecodeError,
};

// ---------------------------------------------------------------------------
// IDL type model
// ---------------------------------------------------------------------------

/// Normalized IDL type, version-independent.
#[derive(Debug, Clone, PartialEq)]
pub enum IdlType {
    Bool,
    U8,
    U16,
    U32,
    U64,
    U128,
    I8,
    I16,
    I32,
    I64,
    I128,
    F32,
    F64,
    Pubkey,
    String,
    Bytes,
    Option(Box<IdlType>),
    Vec(Box<IdlType>),
    Array(Box<IdlType>, usize),
    Defined(String),
}

/// A type definition (struct or enum) from the IDL.
#[derive(Debug, Clone)]
pub enum IdlTypeDef {
    Struct { fields: Vec<(String, IdlType)> },
    Enum { variants: Vec<IdlEnumVariant> },
}

/// A variant of an IDL enum type.
#[derive(Debug, Clone)]
pub struct IdlEnumVariant {
    pub name: String,
    pub fields: Option<Vec<(String, IdlType)>>,
}

/// Parsed instruction definition from the IDL.
pub struct InstructionDef {
    pub name: String,
    pub args: Vec<(String, IdlType)>,
    pub account_names: Vec<String>,
}

/// Fully parsed IDL with discriminator-indexed lookup maps.
pub struct ParsedIdl {
    pub program_name: String,
    pub defined_types: HashMap<String, IdlTypeDef>,
    /// event discriminator -> (event_name, fields)
    pub event_matchers: HashMap<[u8; 8], (String, Vec<(String, IdlType)>)>,
    /// instruction discriminator -> InstructionDef
    pub instruction_matchers: HashMap<[u8; 8], InstructionDef>,
    /// account discriminator -> (account_type_name, fields)
    pub account_matchers: HashMap<[u8; 8], (String, Vec<(String, IdlType)>)>,
}

// ---------------------------------------------------------------------------
// IDL version detection
// ---------------------------------------------------------------------------

/// Detected IDL format version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IdlVersion {
    /// Pre-0.30 (legacy): `publicKey`, `{"defined": "Name"}`, no explicit discriminators.
    Legacy,
    /// 0.30+: `pubkey`, `{"defined": {"name": "Name"}}`, optional explicit discriminators.
    V030,
}

fn detect_version(root: &serde_json::Value) -> IdlVersion {
    // Check for metadata.spec (0.30+ marker).
    if root
        .get("metadata")
        .and_then(|m| m.get("spec"))
        .is_some()
    {
        return IdlVersion::V030;
    }

    // Check for explicit discriminator arrays in any entry.
    for key in &["events", "instructions", "accounts"] {
        if let Some(arr) = root.get(key).and_then(|v| v.as_array()) {
            for item in arr {
                if item.get("discriminator").is_some() {
                    return IdlVersion::V030;
                }
            }
        }
    }

    IdlVersion::Legacy
}

// ---------------------------------------------------------------------------
// Discriminator computation
// ---------------------------------------------------------------------------

const DISCRIMINATOR_LEN: usize = 8;

fn compute_event_discriminator(event_name: &str) -> [u8; DISCRIMINATOR_LEN] {
    let preimage = format!("event:{}", event_name);
    let hash = sha256(preimage.as_bytes());
    let mut disc = [0u8; DISCRIMINATOR_LEN];
    disc.copy_from_slice(&hash.to_bytes()[..DISCRIMINATOR_LEN]);
    disc
}

fn compute_instruction_discriminator(instruction_name: &str) -> [u8; DISCRIMINATOR_LEN] {
    let preimage = format!("global:{}", instruction_name);
    let hash = sha256(preimage.as_bytes());
    let mut disc = [0u8; DISCRIMINATOR_LEN];
    disc.copy_from_slice(&hash.to_bytes()[..DISCRIMINATOR_LEN]);
    disc
}

fn compute_account_discriminator(account_name: &str) -> [u8; DISCRIMINATOR_LEN] {
    let preimage = format!("account:{}", account_name);
    let hash = sha256(preimage.as_bytes());
    let mut disc = [0u8; DISCRIMINATOR_LEN];
    disc.copy_from_slice(&hash.to_bytes()[..DISCRIMINATOR_LEN]);
    disc
}

/// Parse an explicit discriminator array from a 0.30+ IDL entry, if present.
fn parse_explicit_discriminator(value: &serde_json::Value) -> Option<[u8; DISCRIMINATOR_LEN]> {
    let arr = value.get("discriminator")?.as_array()?;
    if arr.len() != DISCRIMINATOR_LEN {
        return None;
    }
    let mut disc = [0u8; DISCRIMINATOR_LEN];
    for (i, v) in arr.iter().enumerate() {
        disc[i] = v.as_u64()? as u8;
    }
    Some(disc)
}

// ---------------------------------------------------------------------------
// IDL type parsing
// ---------------------------------------------------------------------------

fn parse_idl_type(
    value: &serde_json::Value,
    version: IdlVersion,
) -> Result<IdlType, SolanaDecodeError> {
    // String form: primitive types.
    if let Some(s) = value.as_str() {
        return match s {
            "bool" => Ok(IdlType::Bool),
            "u8" => Ok(IdlType::U8),
            "u16" => Ok(IdlType::U16),
            "u32" => Ok(IdlType::U32),
            "u64" => Ok(IdlType::U64),
            "u128" => Ok(IdlType::U128),
            "i8" => Ok(IdlType::I8),
            "i16" => Ok(IdlType::I16),
            "i32" => Ok(IdlType::I32),
            "i64" => Ok(IdlType::I64),
            "i128" => Ok(IdlType::I128),
            "f32" => Ok(IdlType::F32),
            "f64" => Ok(IdlType::F64),
            "string" => Ok(IdlType::String),
            "bytes" => Ok(IdlType::Bytes),
            // 0.30+ uses "pubkey", legacy uses "publicKey"
            "pubkey" => Ok(IdlType::Pubkey),
            "publicKey" => Ok(IdlType::Pubkey),
            other => Err(SolanaDecodeError::IdlParse(format!(
                "unknown primitive type: {}",
                other
            ))),
        };
    }

    // Object form: compound/reference types.
    let obj = value.as_object().ok_or_else(|| {
        SolanaDecodeError::IdlParse(format!("expected string or object for IDL type, got: {}", value))
    })?;

    if let Some(inner) = obj.get("option") {
        let inner_type = parse_idl_type(inner, version)?;
        return Ok(IdlType::Option(Box::new(inner_type)));
    }

    if let Some(inner) = obj.get("vec") {
        let inner_type = parse_idl_type(inner, version)?;
        return Ok(IdlType::Vec(Box::new(inner_type)));
    }

    if let Some(arr_val) = obj.get("array") {
        let arr = arr_val.as_array().ok_or_else(|| {
            SolanaDecodeError::IdlParse("array type must be a JSON array [type, size]".to_string())
        })?;
        if arr.len() != 2 {
            return Err(SolanaDecodeError::IdlParse(
                "array type must have exactly 2 elements".to_string(),
            ));
        }
        let inner_type = parse_idl_type(&arr[0], version)?;
        let size = arr[1].as_u64().ok_or_else(|| {
            SolanaDecodeError::IdlParse("array size must be a number".to_string())
        })? as usize;
        return Ok(IdlType::Array(Box::new(inner_type), size));
    }

    if let Some(defined) = obj.get("defined") {
        match version {
            IdlVersion::V030 => {
                // 0.30+: {"defined": {"name": "TypeName"}}
                if let Some(name_obj) = defined.as_object() {
                    let name = name_obj
                        .get("name")
                        .and_then(|n| n.as_str())
                        .ok_or_else(|| {
                            SolanaDecodeError::IdlParse(
                                "defined type object must have 'name' field".to_string(),
                            )
                        })?;
                    return Ok(IdlType::Defined(name.to_string()));
                }
                // Fallback: some 0.30+ IDLs may still use the string form.
                if let Some(name) = defined.as_str() {
                    return Ok(IdlType::Defined(name.to_string()));
                }
                return Err(SolanaDecodeError::IdlParse(
                    "invalid defined type format in 0.30+ IDL".to_string(),
                ));
            }
            IdlVersion::Legacy => {
                // Legacy: {"defined": "TypeName"}
                let name = defined.as_str().ok_or_else(|| {
                    SolanaDecodeError::IdlParse(
                        "legacy defined type must be a string".to_string(),
                    )
                })?;
                return Ok(IdlType::Defined(name.to_string()));
            }
        }
    }

    Err(SolanaDecodeError::IdlParse(format!(
        "unrecognized IDL type: {}",
        value
    )))
}

// ---------------------------------------------------------------------------
// Type definition parsing
// ---------------------------------------------------------------------------

fn parse_type_def(
    value: &serde_json::Value,
    version: IdlVersion,
) -> Result<(String, IdlTypeDef), SolanaDecodeError> {
    let name = value
        .get("name")
        .and_then(|n| n.as_str())
        .ok_or_else(|| SolanaDecodeError::IdlParse("type definition missing 'name'".to_string()))?
        .to_string();

    let type_obj = value.get("type").ok_or_else(|| {
        SolanaDecodeError::IdlParse(format!("type definition '{}' missing 'type'", name))
    })?;

    let kind = type_obj
        .get("kind")
        .and_then(|k| k.as_str())
        .ok_or_else(|| {
            SolanaDecodeError::IdlParse(format!(
                "type definition '{}' missing 'type.kind'",
                name
            ))
        })?;

    match kind {
        "struct" => {
            let fields_arr = type_obj
                .get("fields")
                .and_then(|f| f.as_array())
                .ok_or_else(|| {
                    SolanaDecodeError::IdlParse(format!(
                        "struct '{}' missing 'fields' array",
                        name
                    ))
                })?;

            let mut fields = Vec::with_capacity(fields_arr.len());
            for (idx, field) in fields_arr.iter().enumerate() {
                if field.is_object() {
                    // Named field: {"name": "x", "type": "u64"}
                    let field_name = field
                        .get("name")
                        .and_then(|n| n.as_str())
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| format!("field_{}", idx));

                    let field_type_val = field.get("type").ok_or_else(|| {
                        SolanaDecodeError::IdlParse(format!(
                            "field '{}' in struct '{}' missing 'type'",
                            field_name, name
                        ))
                    })?;

                    let field_type = parse_idl_type(field_type_val, version)?;
                    fields.push((field_name, field_type));
                } else {
                    // Unnamed/tuple field: bare type like "u64" or {"vec": "u8"}
                    let field_type = parse_idl_type(field, version)?;
                    fields.push((format!("field_{}", idx), field_type));
                }
            }

            Ok((name, IdlTypeDef::Struct { fields }))
        }
        "enum" => {
            let variants_arr = type_obj
                .get("variants")
                .and_then(|v| v.as_array())
                .ok_or_else(|| {
                    SolanaDecodeError::IdlParse(format!(
                        "enum '{}' missing 'variants' array",
                        name
                    ))
                })?;

            let mut variants = Vec::with_capacity(variants_arr.len());
            for variant in variants_arr {
                let variant_name = variant
                    .get("name")
                    .and_then(|n| n.as_str())
                    .ok_or_else(|| {
                        SolanaDecodeError::IdlParse(format!(
                            "variant in enum '{}' missing 'name'",
                            name
                        ))
                    })?
                    .to_string();

                let fields = if let Some(fields_arr) = variant.get("fields").and_then(|f| f.as_array())
                {
                    if fields_arr.is_empty() {
                        None
                    } else {
                        let mut parsed = Vec::with_capacity(fields_arr.len());
                        for (idx, field) in fields_arr.iter().enumerate() {
                            if field.is_object() {
                                // Named field: {"name": "x", "type": "u64"}
                                let f_name = field
                                    .get("name")
                                    .and_then(|n| n.as_str())
                                    .unwrap_or("")
                                    .to_string();
                                let f_name = if f_name.is_empty() {
                                    format!("field_{}", idx)
                                } else {
                                    f_name
                                };
                                let f_type_val = field.get("type").ok_or_else(|| {
                                    SolanaDecodeError::IdlParse(format!(
                                        "variant field '{}' in enum '{}::{}' missing 'type'",
                                        f_name, name, variant_name
                                    ))
                                })?;
                                let f_type = parse_idl_type(f_type_val, version)?;
                                parsed.push((f_name, f_type));
                            } else {
                                // Unnamed/tuple field: bare type like "u64" or {"vec": "u8"}
                                let f_type = parse_idl_type(field, version)?;
                                parsed.push((format!("field_{}", idx), f_type));
                            }
                        }
                        Some(parsed)
                    }
                } else {
                    None
                };

                variants.push(IdlEnumVariant {
                    name: variant_name,
                    fields,
                });
            }

            Ok((name, IdlTypeDef::Enum { variants }))
        }
        other => Err(SolanaDecodeError::IdlParse(format!(
            "unknown type kind '{}' in type '{}'",
            other, name
        ))),
    }
}

// ---------------------------------------------------------------------------
// Top-level IDL parser
// ---------------------------------------------------------------------------

/// Parse an Anchor IDL JSON string into a `ParsedIdl`.
///
/// Supports both pre-0.30 and 0.30+ IDL formats.
pub fn parse_idl(json: &str) -> Result<ParsedIdl, SolanaDecodeError> {
    let root: serde_json::Value = serde_json::from_str(json)
        .map_err(|e| SolanaDecodeError::IdlParse(format!("invalid JSON: {}", e)))?;

    let version = detect_version(&root);

    let program_name = root
        .get("name")
        .or_else(|| root.get("metadata").and_then(|m| m.get("name")))
        .and_then(|n| n.as_str())
        .unwrap_or("unknown")
        .to_string();

    // --- Parse type definitions ---
    let mut defined_types = HashMap::new();
    if let Some(types_arr) = root.get("types").and_then(|t| t.as_array()) {
        for type_val in types_arr {
            let (name, def) = parse_type_def(type_val, version)?;
            defined_types.insert(name, def);
        }
    }

    // Also parse accounts as type definitions (0.30+ puts struct defs in accounts).
    // We do this first to populate defined_types before building matchers.
    if let Some(accounts_arr) = root.get("accounts").and_then(|a| a.as_array()) {
        for acc_val in accounts_arr {
            // 0.30+ accounts have a nested "type" with struct fields.
            // Legacy accounts may also have a "type" section.
            if acc_val.get("type").is_some() {
                if let Ok((name, def)) = parse_type_def(acc_val, version) {
                    defined_types.insert(name, def);
                }
            }
        }
    }

    // --- Parse events ---
    let mut event_matchers = HashMap::new();
    if let Some(events_arr) = root.get("events").and_then(|e| e.as_array()) {
        for event_val in events_arr {
            let event_name = event_val
                .get("name")
                .and_then(|n| n.as_str())
                .ok_or_else(|| {
                    SolanaDecodeError::IdlParse("event missing 'name'".to_string())
                })?
                .to_string();

            let fields = parse_fields_array(event_val, version)?;

            let disc = match parse_explicit_discriminator(event_val) {
                Some(d) => d,
                None => compute_event_discriminator(&event_name),
            };

            event_matchers.insert(disc, (event_name, fields));
        }
    }

    // --- Parse instructions ---
    let mut instruction_matchers = HashMap::new();
    if let Some(ix_arr) = root.get("instructions").and_then(|i| i.as_array()) {
        for ix_val in ix_arr {
            let ix_name = ix_val
                .get("name")
                .and_then(|n| n.as_str())
                .ok_or_else(|| {
                    SolanaDecodeError::IdlParse("instruction missing 'name'".to_string())
                })?
                .to_string();

            let args = parse_args_array(ix_val, version)?;

            let account_names = parse_account_names(ix_val);

            let disc = match parse_explicit_discriminator(ix_val) {
                Some(d) => d,
                None => compute_instruction_discriminator(&ix_name),
            };

            instruction_matchers.insert(
                disc,
                InstructionDef {
                    name: ix_name,
                    args,
                    account_names,
                },
            );
        }
    }

    // --- Parse accounts (as matchers) ---
    let mut account_matchers = HashMap::new();
    if let Some(accounts_arr) = root.get("accounts").and_then(|a| a.as_array()) {
        for acc_val in accounts_arr {
            let acc_name = acc_val
                .get("name")
                .and_then(|n| n.as_str())
                .ok_or_else(|| {
                    SolanaDecodeError::IdlParse("account missing 'name'".to_string())
                })?
                .to_string();

            // Get fields from the type definition if present, or from the defined_types map.
            let fields = if acc_val.get("type").is_some() {
                // Extract fields from the inline type definition.
                let type_obj = acc_val.get("type").unwrap();
                if let Some(fields_arr) = type_obj.get("fields").and_then(|f| f.as_array()) {
                    let mut parsed = Vec::with_capacity(fields_arr.len());
                    for field in fields_arr {
                        let field_name = field
                            .get("name")
                            .and_then(|n| n.as_str())
                            .ok_or_else(|| {
                                SolanaDecodeError::IdlParse(format!(
                                    "field in account '{}' missing 'name'",
                                    acc_name
                                ))
                            })?
                            .to_string();
                        let field_type_val = field.get("type").ok_or_else(|| {
                            SolanaDecodeError::IdlParse(format!(
                                "field '{}' in account '{}' missing 'type'",
                                field_name, acc_name
                            ))
                        })?;
                        let field_type = parse_idl_type(field_type_val, version)?;
                        parsed.push((field_name, field_type));
                    }
                    parsed
                } else {
                    Vec::new()
                }
            } else if let Some(IdlTypeDef::Struct { fields }) = defined_types.get(&acc_name) {
                fields.clone()
            } else {
                Vec::new()
            };

            let disc = match parse_explicit_discriminator(acc_val) {
                Some(d) => d,
                None => compute_account_discriminator(&acc_name),
            };

            account_matchers.insert(disc, (acc_name, fields));
        }
    }

    Ok(ParsedIdl {
        program_name,
        defined_types,
        event_matchers,
        instruction_matchers,
        account_matchers,
    })
}

// ---------------------------------------------------------------------------
// Field/arg parsing helpers
// ---------------------------------------------------------------------------

/// Parse the "fields" array from an event definition.
fn parse_fields_array(
    value: &serde_json::Value,
    version: IdlVersion,
) -> Result<Vec<(String, IdlType)>, SolanaDecodeError> {
    let fields_arr = match value.get("fields").and_then(|f| f.as_array()) {
        Some(arr) => arr,
        None => return Ok(Vec::new()),
    };

    let mut fields = Vec::with_capacity(fields_arr.len());
    for field in fields_arr {
        let name = field
            .get("name")
            .and_then(|n| n.as_str())
            .ok_or_else(|| SolanaDecodeError::IdlParse("field missing 'name'".to_string()))?
            .to_string();
        let type_val = field
            .get("type")
            .ok_or_else(|| SolanaDecodeError::IdlParse("field missing 'type'".to_string()))?;
        let idl_type = parse_idl_type(type_val, version)?;
        fields.push((name, idl_type));
    }
    Ok(fields)
}

/// Parse the "args" array from an instruction definition.
fn parse_args_array(
    value: &serde_json::Value,
    version: IdlVersion,
) -> Result<Vec<(String, IdlType)>, SolanaDecodeError> {
    let args_arr = match value.get("args").and_then(|a| a.as_array()) {
        Some(arr) => arr,
        None => return Ok(Vec::new()),
    };

    let mut args = Vec::with_capacity(args_arr.len());
    for arg in args_arr {
        let name = arg
            .get("name")
            .and_then(|n| n.as_str())
            .ok_or_else(|| SolanaDecodeError::IdlParse("arg missing 'name'".to_string()))?
            .to_string();
        let type_val = arg
            .get("type")
            .ok_or_else(|| SolanaDecodeError::IdlParse("arg missing 'type'".to_string()))?;
        let idl_type = parse_idl_type(type_val, version)?;
        args.push((name, idl_type));
    }
    Ok(args)
}

/// Extract account names from an instruction definition, flattening composite
/// account groups (nested `accounts` arrays) into leaf accounts.
fn parse_account_names(value: &serde_json::Value) -> Vec<String> {
    let accounts_arr = match value.get("accounts").and_then(|a| a.as_array()) {
        Some(arr) => arr,
        None => return Vec::new(),
    };

    let mut names = Vec::new();
    collect_leaf_account_names(accounts_arr, &mut names);
    names
}

fn collect_leaf_account_names(accounts: &[serde_json::Value], out: &mut Vec<String>) {
    for acc in accounts {
        // Composite group: has nested "accounts" array — recurse into it.
        if let Some(nested) = acc.get("accounts").and_then(|a| a.as_array()) {
            collect_leaf_account_names(nested, out);
        } else if let Some(name) = acc.get("name").and_then(|n| n.as_str()) {
            out.push(name.to_string());
        }
    }
}

// ---------------------------------------------------------------------------
// AnchorDecoder
// ---------------------------------------------------------------------------

/// IDL-driven decoder implementing `ProgramDecoder`.
pub struct AnchorDecoder {
    program_id: [u8; 32],
    #[allow(dead_code)]
    program_name: String,
    parsed_idl: ParsedIdl,
}

impl AnchorDecoder {
    /// Create a new `AnchorDecoder` from a program ID, human name, and IDL JSON.
    pub fn new(
        program_id: [u8; 32],
        program_name: String,
        idl_json: &str,
    ) -> Result<Self, SolanaDecodeError> {
        let parsed_idl = parse_idl(idl_json)?;
        Ok(Self {
            program_id,
            program_name,
            parsed_idl,
        })
    }
}

impl ProgramDecoder for AnchorDecoder {
    fn program_id(&self) -> [u8; 32] {
        self.program_id
    }

    fn program_name(&self) -> &str {
        &self.parsed_idl.program_name
    }

    fn decode_event(
        &self,
        discriminator: &[u8],
        data: &[u8],
    ) -> Result<Option<DecodedEventFields>, SolanaDecodeError> {
        if discriminator.len() < 8 {
            return Ok(None);
        }
        let mut disc = [0u8; 8];
        disc.copy_from_slice(&discriminator[..8]);

        let (event_name, fields) = match self.parsed_idl.event_matchers.get(&disc) {
            Some(entry) => entry,
            None => return Ok(None),
        };

        let mut cursor: &[u8] = data;
        let params =
            borsh_dynamic::deserialize_struct_fields(&mut cursor, fields, &self.parsed_idl.defined_types)?;

        Ok(Some(DecodedEventFields {
            event_name: event_name.clone(),
            params,
        }))
    }

    fn decode_instruction(
        &self,
        data: &[u8],
        accounts: &[[u8; 32]],
    ) -> Result<Option<DecodedInstructionFields>, SolanaDecodeError> {
        if data.len() < 8 {
            return Ok(None);
        }

        let mut disc = [0u8; 8];
        disc.copy_from_slice(&data[..8]);
        let remaining = &data[8..];

        let ix_def = match self.parsed_idl.instruction_matchers.get(&disc) {
            Some(def) => def,
            None => return Ok(None),
        };

        let mut cursor: &[u8] = remaining;
        let args = borsh_dynamic::deserialize_struct_fields(
            &mut cursor,
            &ix_def.args,
            &self.parsed_idl.defined_types,
        )?;

        let mut named_accounts = HashMap::new();
        for (i, account_name) in ix_def.account_names.iter().enumerate() {
            if let Some(pubkey) = accounts.get(i) {
                named_accounts.insert(account_name.clone(), *pubkey);
            }
        }

        Ok(Some(DecodedInstructionFields {
            instruction_name: ix_def.name.clone(),
            args,
            named_accounts,
        }))
    }

    fn decode_account(
        &self,
        data: &[u8],
    ) -> Result<Option<DecodedAccountFields>, SolanaDecodeError> {
        if data.len() < 8 {
            return Ok(None);
        }

        let mut disc = [0u8; 8];
        disc.copy_from_slice(&data[..8]);
        let remaining = &data[8..];

        let (account_type, field_defs) = match self.parsed_idl.account_matchers.get(&disc) {
            Some(entry) => entry,
            None => return Ok(None),
        };

        let mut cursor: &[u8] = remaining;
        let fields = borsh_dynamic::deserialize_struct_fields(
            &mut cursor,
            field_defs,
            &self.parsed_idl.defined_types,
        )?;

        Ok(Some(DecodedAccountFields {
            account_type: account_type.clone(),
            fields,
        }))
    }

    fn event_types(&self) -> Vec<String> {
        self.parsed_idl
            .event_matchers
            .values()
            .map(|(name, _)| name.clone())
            .collect()
    }

    fn instruction_types(&self) -> Vec<String> {
        self.parsed_idl
            .instruction_matchers
            .values()
            .map(|def| def.name.clone())
            .collect()
    }

    fn account_types(&self) -> Vec<String> {
        self.parsed_idl
            .account_matchers
            .values()
            .map(|(name, _)| name.clone())
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Minimal 0.30+ IDL JSON
    // -----------------------------------------------------------------------

    fn v030_idl_json() -> &'static str {
        r#"{
            "address": "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
            "metadata": {
                "name": "test_program",
                "spec": "0.1.0"
            },
            "instructions": [
                {
                    "name": "initialize",
                    "discriminator": [175, 175, 109, 31, 13, 152, 155, 237],
                    "accounts": [
                        {"name": "pool", "writable": true},
                        {"name": "authority", "signer": true}
                    ],
                    "args": [
                        {"name": "tick_spacing", "type": "u16"},
                        {"name": "initial_sqrt_price", "type": "u128"}
                    ]
                }
            ],
            "accounts": [
                {
                    "name": "Pool",
                    "discriminator": [241, 154, 109, 4, 17, 177, 109, 188],
                    "type": {
                        "kind": "struct",
                        "fields": [
                            {"name": "authority", "type": "pubkey"},
                            {"name": "tick_spacing", "type": "u16"},
                            {"name": "liquidity", "type": "u128"}
                        ]
                    }
                }
            ],
            "events": [
                {
                    "name": "PoolInitialized",
                    "discriminator": [50, 40, 30, 20, 10, 5, 3, 1],
                    "fields": [
                        {"name": "pool", "type": "pubkey"},
                        {"name": "tick_spacing", "type": "u16"}
                    ]
                }
            ],
            "types": [
                {
                    "name": "TickData",
                    "type": {
                        "kind": "struct",
                        "fields": [
                            {"name": "index", "type": "i32"},
                            {"name": "liquidity_net", "type": "i128"}
                        ]
                    }
                }
            ]
        }"#
    }

    // -----------------------------------------------------------------------
    // Minimal legacy (pre-0.30) IDL JSON
    // -----------------------------------------------------------------------

    fn legacy_idl_json() -> &'static str {
        r#"{
            "version": "0.28.0",
            "name": "legacy_program",
            "instructions": [
                {
                    "name": "swap",
                    "accounts": [
                        {"name": "pool", "isMut": true, "isSigner": false},
                        {"name": "user", "isMut": false, "isSigner": true}
                    ],
                    "args": [
                        {"name": "amount", "type": "u64"},
                        {"name": "other_amount_threshold", "type": "u64"},
                        {"name": "sqrt_price_limit", "type": "u128"},
                        {"name": "amount_specified_is_input", "type": "bool"},
                        {"name": "a_to_b", "type": "bool"}
                    ]
                }
            ],
            "accounts": [
                {
                    "name": "Whirlpool",
                    "type": {
                        "kind": "struct",
                        "fields": [
                            {"name": "whirlpoolsConfig", "type": "publicKey"},
                            {"name": "tickSpacing", "type": "u16"}
                        ]
                    }
                }
            ],
            "events": [
                {
                    "name": "Swapped",
                    "fields": [
                        {"name": "pool", "type": "publicKey"},
                        {"name": "amount", "type": "u64"}
                    ]
                }
            ],
            "types": [
                {
                    "name": "SwapParams",
                    "type": {
                        "kind": "struct",
                        "fields": [
                            {"name": "pool", "type": "publicKey"},
                            {"name": "amount", "type": "u64"}
                        ]
                    }
                }
            ]
        }"#
    }

    // -----------------------------------------------------------------------
    // Test: parse 0.30+ IDL
    // -----------------------------------------------------------------------

    #[test]
    fn test_parse_v030_idl() {
        let parsed = parse_idl(v030_idl_json()).unwrap();

        assert_eq!(parsed.program_name, "test_program");

        // Events
        assert_eq!(parsed.event_matchers.len(), 1);
        let disc = [50, 40, 30, 20, 10, 5, 3, 1];
        let (event_name, event_fields) = &parsed.event_matchers[&disc];
        assert_eq!(event_name, "PoolInitialized");
        assert_eq!(event_fields.len(), 2);
        assert_eq!(event_fields[0].0, "pool");
        assert_eq!(event_fields[0].1, IdlType::Pubkey);
        assert_eq!(event_fields[1].0, "tick_spacing");
        assert_eq!(event_fields[1].1, IdlType::U16);

        // Instructions
        assert_eq!(parsed.instruction_matchers.len(), 1);
        let ix_disc = [175, 175, 109, 31, 13, 152, 155, 237];
        let ix = &parsed.instruction_matchers[&ix_disc];
        assert_eq!(ix.name, "initialize");
        assert_eq!(ix.args.len(), 2);
        assert_eq!(ix.account_names, vec!["pool", "authority"]);

        // Accounts
        assert_eq!(parsed.account_matchers.len(), 1);
        let acc_disc = [241, 154, 109, 4, 17, 177, 109, 188];
        let (acc_name, acc_fields) = &parsed.account_matchers[&acc_disc];
        assert_eq!(acc_name, "Pool");
        assert_eq!(acc_fields.len(), 3);
        assert_eq!(acc_fields[0].1, IdlType::Pubkey);

        // Types
        assert!(parsed.defined_types.contains_key("TickData"));
    }

    // -----------------------------------------------------------------------
    // Test: parse legacy IDL
    // -----------------------------------------------------------------------

    #[test]
    fn test_parse_legacy_idl() {
        let parsed = parse_idl(legacy_idl_json()).unwrap();

        assert_eq!(parsed.program_name, "legacy_program");

        // Events (discriminators are computed, not explicit).
        assert_eq!(parsed.event_matchers.len(), 1);
        let expected_disc = compute_event_discriminator("Swapped");
        let (event_name, event_fields) = &parsed.event_matchers[&expected_disc];
        assert_eq!(event_name, "Swapped");
        assert_eq!(event_fields.len(), 2);
        // Legacy "publicKey" should parse to Pubkey.
        assert_eq!(event_fields[0].1, IdlType::Pubkey);

        // Instructions (discriminators are computed).
        assert_eq!(parsed.instruction_matchers.len(), 1);
        let swap_disc = compute_instruction_discriminator("swap");
        let ix = &parsed.instruction_matchers[&swap_disc];
        assert_eq!(ix.name, "swap");
        assert_eq!(ix.args.len(), 5);
        assert_eq!(ix.account_names, vec!["pool", "user"]);

        // Accounts.
        let whirlpool_disc = compute_account_discriminator("Whirlpool");
        let (acc_name, acc_fields) = &parsed.account_matchers[&whirlpool_disc];
        assert_eq!(acc_name, "Whirlpool");
        assert_eq!(acc_fields.len(), 2);
        assert_eq!(acc_fields[0].0, "whirlpoolsConfig");
        assert_eq!(acc_fields[0].1, IdlType::Pubkey);

        // Types with publicKey.
        if let Some(IdlTypeDef::Struct { fields }) = parsed.defined_types.get("SwapParams") {
            assert_eq!(fields[0].1, IdlType::Pubkey);
        } else {
            panic!("SwapParams should be a struct in defined_types");
        }
    }

    // -----------------------------------------------------------------------
    // Test: version detection
    // -----------------------------------------------------------------------

    #[test]
    fn test_version_detection() {
        let v030_root: serde_json::Value = serde_json::from_str(v030_idl_json()).unwrap();
        assert_eq!(detect_version(&v030_root), IdlVersion::V030);

        let legacy_root: serde_json::Value = serde_json::from_str(legacy_idl_json()).unwrap();
        assert_eq!(detect_version(&legacy_root), IdlVersion::Legacy);

        // A minimal object with no metadata.spec and no discriminator arrays => Legacy.
        let minimal: serde_json::Value =
            serde_json::from_str(r#"{"name": "test", "instructions": []}"#).unwrap();
        assert_eq!(detect_version(&minimal), IdlVersion::Legacy);
    }

    // -----------------------------------------------------------------------
    // Test: discriminator computation
    // -----------------------------------------------------------------------

    #[test]
    fn test_discriminator_computation() {
        // Event discriminators.
        let event_disc = compute_event_discriminator("PoolInitialized");
        let expected_hash = sha256(b"event:PoolInitialized");
        let expected: [u8; 8] = expected_hash.to_bytes()[..8].try_into().unwrap();
        assert_eq!(event_disc, expected);

        // Instruction discriminators.
        let ix_disc = compute_instruction_discriminator("initialize");
        let expected_hash = sha256(b"global:initialize");
        let expected: [u8; 8] = expected_hash.to_bytes()[..8].try_into().unwrap();
        assert_eq!(ix_disc, expected);

        // Account discriminators.
        let acc_disc = compute_account_discriminator("Whirlpool");
        let expected_hash = sha256(b"account:Whirlpool");
        let expected: [u8; 8] = expected_hash.to_bytes()[..8].try_into().unwrap();
        assert_eq!(acc_disc, expected);

        // Different names yield different discriminators.
        assert_ne!(
            compute_event_discriminator("Foo"),
            compute_event_discriminator("Bar")
        );
    }

    // -----------------------------------------------------------------------
    // Test: defined type resolution
    // -----------------------------------------------------------------------

    #[test]
    fn test_defined_type_resolution() {
        let idl_json = r#"{
            "name": "ref_test",
            "instructions": [],
            "events": [
                {
                    "name": "MyEvent",
                    "fields": [
                        {"name": "data", "type": {"defined": "MyData"}}
                    ]
                }
            ],
            "types": [
                {
                    "name": "MyData",
                    "type": {
                        "kind": "struct",
                        "fields": [
                            {"name": "value", "type": "u64"}
                        ]
                    }
                }
            ]
        }"#;

        let parsed = parse_idl(idl_json).unwrap();

        // The event field should reference Defined("MyData").
        let disc = compute_event_discriminator("MyEvent");
        let (_name, fields) = &parsed.event_matchers[&disc];
        assert_eq!(fields[0].1, IdlType::Defined("MyData".to_string()));

        // The defined type should be in defined_types.
        assert!(parsed.defined_types.contains_key("MyData"));
    }

    // -----------------------------------------------------------------------
    // Test: AnchorDecoder decode_event
    // -----------------------------------------------------------------------

    #[test]
    fn test_anchor_decoder_decode_event() {
        let idl_json = r#"{
            "name": "test_decoder",
            "instructions": [],
            "events": [
                {
                    "name": "Transfer",
                    "fields": [
                        {"name": "amount", "type": "u64"},
                        {"name": "sender", "type": "publicKey"}
                    ]
                }
            ],
            "types": []
        }"#;

        let decoder = AnchorDecoder::new([0u8; 32], "test".to_string(), idl_json).unwrap();

        // Build event data: amount(u64) + sender(pubkey).
        let mut event_data = Vec::new();
        event_data.extend_from_slice(&1000u64.to_le_bytes());
        event_data.extend_from_slice(&[0xAA; 32]);

        let disc = compute_event_discriminator("Transfer");
        let result = decoder.decode_event(&disc, &event_data).unwrap();

        let decoded = result.expect("should decode known event");
        assert_eq!(decoded.event_name, "Transfer");
        assert_eq!(decoded.params["amount"].as_u64(), Some(1000));
        assert_eq!(decoded.params["sender"].as_pubkey(), Some([0xAA; 32]));
    }

    // -----------------------------------------------------------------------
    // Test: AnchorDecoder unknown discriminator
    // -----------------------------------------------------------------------

    #[test]
    fn test_anchor_decoder_unknown_discriminator() {
        let idl_json = r#"{
            "name": "test_decoder",
            "instructions": [],
            "events": [
                {
                    "name": "Transfer",
                    "fields": [
                        {"name": "amount", "type": "u64"}
                    ]
                }
            ],
            "types": []
        }"#;

        let decoder = AnchorDecoder::new([0u8; 32], "test".to_string(), idl_json).unwrap();

        let unknown_disc = [0xFF; 8];
        let result = decoder.decode_event(&unknown_disc, &[]).unwrap();
        assert!(result.is_none());

        // Unknown instruction (data with bogus discriminator).
        let mut bogus_data = Vec::new();
        bogus_data.extend_from_slice(&[0xFF; 8]); // discriminator
        let ix_result = decoder.decode_instruction(&bogus_data, &[]).unwrap();
        assert!(ix_result.is_none());

        // Unknown account.
        let mut bogus_account = Vec::new();
        bogus_account.extend_from_slice(&[0xFF; 8]); // discriminator
        let acc_result = decoder.decode_account(&bogus_account).unwrap();
        assert!(acc_result.is_none());
    }

    // -----------------------------------------------------------------------
    // Test: AnchorDecoder decode_instruction
    // -----------------------------------------------------------------------

    #[test]
    fn test_anchor_decoder_decode_instruction() {
        let idl_json = r#"{
            "name": "test_decoder",
            "instructions": [
                {
                    "name": "transfer",
                    "accounts": [
                        {"name": "source", "isMut": true, "isSigner": false},
                        {"name": "destination", "isMut": true, "isSigner": false},
                        {"name": "authority", "isMut": false, "isSigner": true}
                    ],
                    "args": [
                        {"name": "amount", "type": "u64"},
                        {"name": "bump", "type": "u8"}
                    ]
                }
            ],
            "events": [],
            "types": []
        }"#;

        let decoder = AnchorDecoder::new([0u8; 32], "test".to_string(), idl_json).unwrap();

        // Build instruction data: discriminator + amount(u64) + bump(u8).
        let disc = compute_instruction_discriminator("transfer");
        let mut ix_data = Vec::new();
        ix_data.extend_from_slice(&disc);
        ix_data.extend_from_slice(&500u64.to_le_bytes());
        ix_data.push(255u8);

        let accounts = [
            [1u8; 32], // source
            [2u8; 32], // destination
            [3u8; 32], // authority
        ];

        let result = decoder.decode_instruction(&ix_data, &accounts).unwrap();
        let decoded = result.expect("should decode known instruction");

        assert_eq!(decoded.instruction_name, "transfer");
        assert_eq!(decoded.args["amount"].as_u64(), Some(500));
        assert_eq!(decoded.args["bump"].as_u8(), Some(255));
        assert_eq!(decoded.named_accounts["source"], [1u8; 32]);
        assert_eq!(decoded.named_accounts["destination"], [2u8; 32]);
        assert_eq!(decoded.named_accounts["authority"], [3u8; 32]);
    }

    // -----------------------------------------------------------------------
    // Test: composite instruction accounts are flattened
    // -----------------------------------------------------------------------

    #[test]
    fn test_composite_account_flattening() {
        let idl_json = r#"{
            "name": "composite_test",
            "instructions": [
                {
                    "name": "swap",
                    "accounts": [
                        {"name": "pool", "isMut": true, "isSigner": false},
                        {
                            "name": "tokenGroup",
                            "accounts": [
                                {"name": "tokenA", "isMut": true, "isSigner": false},
                                {"name": "tokenB", "isMut": true, "isSigner": false}
                            ]
                        },
                        {"name": "authority", "isMut": false, "isSigner": true}
                    ],
                    "args": [
                        {"name": "amount", "type": "u64"}
                    ]
                }
            ],
            "events": [],
            "types": []
        }"#;

        let decoder = AnchorDecoder::new([0u8; 32], "test".to_string(), idl_json).unwrap();
        let disc = compute_instruction_discriminator("swap");
        let mut ix_data = Vec::new();
        ix_data.extend_from_slice(&disc);
        ix_data.extend_from_slice(&100u64.to_le_bytes());

        let accounts = [
            [1u8; 32], // pool
            [2u8; 32], // tokenA (inside tokenGroup)
            [3u8; 32], // tokenB (inside tokenGroup)
            [4u8; 32], // authority
        ];

        let result = decoder.decode_instruction(&ix_data, &accounts).unwrap();
        let decoded = result.expect("should decode");

        // Composite group "tokenGroup" should be flattened to leaf accounts.
        assert_eq!(decoded.named_accounts.len(), 4);
        assert_eq!(decoded.named_accounts["pool"], [1u8; 32]);
        assert_eq!(decoded.named_accounts["tokenA"], [2u8; 32]);
        assert_eq!(decoded.named_accounts["tokenB"], [3u8; 32]);
        assert_eq!(decoded.named_accounts["authority"], [4u8; 32]);
    }

    // -----------------------------------------------------------------------
    // Test: unnamed enum variant fields (tuple-style)
    // -----------------------------------------------------------------------

    #[test]
    fn test_unnamed_enum_variant_fields() {
        let idl_json = r#"{
            "name": "enum_test",
            "instructions": [],
            "events": [],
            "types": [
                {
                    "name": "OrderStatus",
                    "type": {
                        "kind": "enum",
                        "variants": [
                            {"name": "Pending"},
                            {"name": "Filled", "fields": ["u64", "u64"]},
                            {"name": "Partial", "fields": [
                                {"name": "filled", "type": "u64"},
                                {"name": "remaining", "type": "u64"}
                            ]}
                        ]
                    }
                }
            ]
        }"#;

        let parsed = parse_idl(idl_json).unwrap();
        let type_def = parsed.defined_types.get("OrderStatus").expect("type present");

        if let IdlTypeDef::Enum { variants } = type_def {
            assert_eq!(variants.len(), 3);

            // Unit variant
            assert_eq!(variants[0].name, "Pending");
            assert!(variants[0].fields.is_none());

            // Unnamed/tuple variant
            assert_eq!(variants[1].name, "Filled");
            let fields = variants[1].fields.as_ref().unwrap();
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].0, "field_0");
            assert_eq!(fields[0].1, IdlType::U64);
            assert_eq!(fields[1].0, "field_1");
            assert_eq!(fields[1].1, IdlType::U64);

            // Named variant (still works)
            assert_eq!(variants[2].name, "Partial");
            let fields = variants[2].fields.as_ref().unwrap();
            assert_eq!(fields[0].0, "filled");
            assert_eq!(fields[1].0, "remaining");
        } else {
            panic!("OrderStatus should be an enum");
        }
    }

    // -----------------------------------------------------------------------
    // Test: tuple-style struct definitions
    // -----------------------------------------------------------------------

    #[test]
    fn test_tuple_struct_fields() {
        let idl_json = r#"{
            "name": "tuple_test",
            "instructions": [],
            "events": [],
            "types": [
                {
                    "name": "Pair",
                    "type": {
                        "kind": "struct",
                        "fields": ["u64", "pubkey"]
                    }
                },
                {
                    "name": "Mixed",
                    "type": {
                        "kind": "struct",
                        "fields": [
                            "u64",
                            {"name": "label", "type": "string"},
                            "bool"
                        ]
                    }
                }
            ]
        }"#;

        let parsed = parse_idl(idl_json).unwrap();

        // Pure tuple struct
        let pair = parsed.defined_types.get("Pair").expect("Pair present");
        if let IdlTypeDef::Struct { fields } = pair {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].0, "field_0");
            assert_eq!(fields[0].1, IdlType::U64);
            assert_eq!(fields[1].0, "field_1");
            assert_eq!(fields[1].1, IdlType::Pubkey);
        } else {
            panic!("Pair should be a struct");
        }

        // Mixed named + unnamed fields
        let mixed = parsed.defined_types.get("Mixed").expect("Mixed present");
        if let IdlTypeDef::Struct { fields } = mixed {
            assert_eq!(fields.len(), 3);
            assert_eq!(fields[0].0, "field_0");
            assert_eq!(fields[0].1, IdlType::U64);
            assert_eq!(fields[1].0, "label");
            assert_eq!(fields[1].1, IdlType::String);
            assert_eq!(fields[2].0, "field_2");
            assert_eq!(fields[2].1, IdlType::Bool);
        } else {
            panic!("Mixed should be a struct");
        }
    }
}
