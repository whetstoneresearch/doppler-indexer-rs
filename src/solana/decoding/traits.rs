use std::collections::HashMap;

use crate::types::decoded::DecodedValue;

/// Errors from Solana program decoding.
#[derive(Debug, Clone, thiserror::Error)]
pub enum SolanaDecodeError {
    #[error("Unexpected end of data: need {needed} bytes, have {available}")]
    UnexpectedEof { needed: usize, available: usize },

    #[error("Unknown defined type: {0}")]
    UnknownType(String),

    #[error("Invalid enum variant index: {0}")]
    InvalidEnumVariant(usize),

    #[error("IDL parse error: {0}")]
    IdlParse(String),
}

/// Decoded event fields from a program decoder.
#[derive(Debug, Clone)]
pub struct DecodedEventFields {
    pub event_name: String,
    pub params: HashMap<String, DecodedValue>,
}

/// Decoded instruction fields from a program decoder.
#[derive(Debug, Clone)]
pub struct DecodedInstructionFields {
    pub instruction_name: String,
    pub args: HashMap<String, DecodedValue>,
    /// Named account references from the IDL (e.g., "pool" → pubkey bytes).
    pub named_accounts: HashMap<String, [u8; 32]>,
}

/// Decoded account state fields from a program decoder.
#[derive(Debug, Clone)]
pub struct DecodedAccountFields {
    pub account_type: String,
    pub fields: HashMap<String, DecodedValue>,
}

/// Trait for decoding a Solana program's events, instructions, and account state.
///
/// Implemented by `AnchorDecoder` (IDL-driven) and hand-written decoders for
/// well-known programs like SPL Token.
pub trait ProgramDecoder: Send + Sync {
    /// Program ID this decoder handles.
    fn program_id(&self) -> [u8; 32];

    /// Human-readable name for logging.
    fn program_name(&self) -> &str;

    /// Decode an event from raw "Program data:" log entry.
    /// Returns `None` if the discriminator doesn't match any known event.
    fn decode_event(
        &self,
        discriminator: &[u8],
        data: &[u8],
    ) -> Result<Option<DecodedEventFields>, SolanaDecodeError>;

    /// Decode instruction data + account keys.
    /// Returns `None` if the discriminator doesn't match any known instruction.
    fn decode_instruction(
        &self,
        data: &[u8],
        accounts: &[[u8; 32]],
    ) -> Result<Option<DecodedInstructionFields>, SolanaDecodeError>;

    /// Decode account state from raw account data.
    /// Returns `None` if the discriminator doesn't match any known account type.
    fn decode_account(
        &self,
        data: &[u8],
    ) -> Result<Option<DecodedAccountFields>, SolanaDecodeError>;

    /// Event types this decoder can produce.
    fn event_types(&self) -> Vec<String>;

    /// Instruction types this decoder can produce.
    fn instruction_types(&self) -> Vec<String>;

    /// Account types this decoder can produce.
    fn account_types(&self) -> Vec<String>;
}
