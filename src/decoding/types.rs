use std::collections::HashMap;
use std::path::PathBuf;

use alloy::primitives::Address;
use tokio::sync::mpsc::Sender;

use crate::live::TransformRetryRequest;
use crate::raw_data::historical::receipts::LogData;
use crate::transformations::{DecodedCallsMessage, DecodedEventsMessage, RangeCompleteMessage};

use super::event_parsing::TupleFieldInfo;
use super::logs::EventMatcher;

/// Output channels for the eth_call decoder.
pub struct EthCallDecoderOutputs<'a> {
    pub transform_tx: Option<&'a Sender<DecodedCallsMessage>>,
    pub complete_tx: Option<&'a Sender<RangeCompleteMessage>>,
    pub retry_tx: Option<&'a Sender<TransformRetryRequest>>,
}

/// Output channels for the log decoder.
pub struct LogDecoderOutputs<'a> {
    pub transform_tx: Option<&'a Sender<DecodedEventsMessage>>,
    pub complete_tx: Option<&'a Sender<RangeCompleteMessage>>,
}

/// Compiled log matchers for regular contracts and factory collections.
pub struct LogMatcherConfig<'a> {
    pub regular_matchers: &'a [EventMatcher],
    pub factory_matchers: &'a HashMap<String, Vec<EventMatcher>>,
}

/// Result of building matchers: (regular_matchers, factory_matchers).
pub type BuiltMatchers = (Vec<EventMatcher>, HashMap<String, Vec<EventMatcher>>);

/// A file to process during catchup: (range_start, range_end, path, regular_matchers, factory_matchers).
pub type FileProcessingEntry = (
    u64,
    u64,
    PathBuf,
    Vec<EventMatcher>,
    HashMap<String, Vec<EventMatcher>>,
);

/// Parse result for tuple fields: (field_info, canonical_type_strings).
pub type TupleFieldParseResult = (Vec<(String, TupleFieldInfo)>, Vec<String>);

/// Raw eth_call result data for decoding
#[derive(Debug, Clone)]
pub struct EthCallResult {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub contract_address: [u8; 20],
    pub value: Vec<u8>,
}

/// "Once" call result with multiple function results
#[derive(Debug, Clone)]
pub struct OnceCallResult {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub contract_address: [u8; 20],
    /// function_name -> raw result bytes
    pub results: HashMap<String, Vec<u8>>,
}

/// Event-triggered eth_call result data for decoding
#[derive(Debug, Clone)]
pub struct EventCallResult {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub log_index: u32,
    pub target_address: [u8; 20],
    pub value: Vec<u8>,
    pub is_reverted: bool,
    pub revert_reason: Option<String>,
}

/// Message sent through decoder channels
#[derive(Debug)]
pub enum DecoderMessage {
    /// Raw log data ready for decoding
    LogsReady {
        range_start: u64,
        range_end: u64,
        logs: std::sync::Arc<Vec<LogData>>,
        /// If true, write to live bincode storage instead of parquet
        live_mode: bool,
        /// If true, decoder should wait for FactoryAddresses before processing
        #[allow(dead_code)]
        has_factory_matchers: bool,
    },
    /// Regular eth_call results ready for decoding
    EthCallsReady {
        range_start: u64,
        range_end: u64,
        contract_name: String,
        function_name: String,
        results: Vec<EthCallResult>,
        /// If true, write to live bincode storage instead of parquet
        live_mode: bool,
        /// If true, persist decoded artifacts but defer transform execution
        /// until the block-complete retry request is processed.
        retry_transform_after_decode: bool,
    },
    /// "Once" eth_call results ready for decoding
    OnceCallsReady {
        range_start: u64,
        range_end: u64,
        contract_name: String,
        results: Vec<OnceCallResult>,
        /// If true, write to live bincode storage instead of parquet
        live_mode: bool,
        /// If true, persist decoded artifacts but defer transform execution
        /// until the block-complete retry request is processed.
        retry_transform_after_decode: bool,
    },
    /// Event-triggered eth_call results ready for decoding
    EventCallsReady {
        range_start: u64,
        range_end: u64,
        contract_name: String,
        function_name: String,
        results: Vec<EventCallResult>,
        /// If true, write to live bincode storage instead of parquet
        live_mode: bool,
        /// If true, persist decoded artifacts but defer transform execution
        /// until the block-complete retry request is processed.
        retry_transform_after_decode: bool,
    },
    /// Marker indicating all eth_call decode work for this block/range has been queued.
    EthCallsBlockComplete {
        range_start: u64,
        range_end: u64,
        retry_transform_after_decode: bool,
    },
    /// Factory addresses discovered for a range (needed for factory log/call decoding)
    #[allow(dead_code)]
    FactoryAddresses {
        range_start: u64,
        range_end: u64,
        /// collection_name -> addresses discovered in this range
        addresses: HashMap<String, Vec<Address>>,
    },
    /// A once-call file was backfilled with new columns - decoder should re-check it
    OnceFileBackfilled {
        range_start: u64,
        range_end: u64,
        contract_name: String,
    },
    /// A reorg was detected - decoder should clean up orphaned data
    Reorg {
        /// The common ancestor block number (last valid block)
        _common_ancestor: u64,
        /// Block numbers that were orphaned and need cleanup
        orphaned: Vec<u64>,
    },
    /// Solana events ready for Borsh decoding (historical + live)
    #[cfg(feature = "solana")]
    SolanaEventsReady {
        range_start: u64,
        range_end: u64,
        events: Vec<crate::solana::raw_data::types::SolanaEventRecord>,
        live_mode: bool,
    },
    /// Solana instructions ready for Borsh decoding (historical + live)
    #[cfg(feature = "solana")]
    SolanaInstructionsReady {
        range_start: u64,
        range_end: u64,
        instructions: Vec<crate::solana::raw_data::types::SolanaInstructionRecord>,
        live_mode: bool,
    },
    /// All ranges complete (shutdown signal)
    AllComplete,
}
