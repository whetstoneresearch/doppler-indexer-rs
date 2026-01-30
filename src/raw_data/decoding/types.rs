use std::collections::HashMap;

use alloy::primitives::Address;

use crate::raw_data::historical::receipts::LogData;

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

/// Message sent through decoder channels
#[derive(Debug)]
pub enum DecoderMessage {
    /// Raw log data ready for decoding
    LogsReady {
        range_start: u64,
        range_end: u64,
        logs: Vec<LogData>,
    },
    /// Regular eth_call results ready for decoding
    EthCallsReady {
        range_start: u64,
        range_end: u64,
        contract_name: String,
        function_name: String,
        results: Vec<EthCallResult>,
    },
    /// "Once" eth_call results ready for decoding
    OnceCallsReady {
        range_start: u64,
        range_end: u64,
        contract_name: String,
        results: Vec<OnceCallResult>,
    },
    /// Factory addresses discovered for a range (needed for factory log/call decoding)
    FactoryAddresses {
        range_start: u64,
        range_end: u64,
        /// collection_name -> addresses discovered in this range
        addresses: HashMap<String, Vec<Address>>,
    },
    /// All ranges complete (shutdown signal)
    AllComplete,
}
