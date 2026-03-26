mod catchup;
mod current;
pub(crate) mod eth_calls;
pub(crate) mod event_parsing;
pub(crate) mod logs;
mod types;

pub use eth_calls::decode_eth_calls;
pub use logs::decode_logs;
pub use types::{DecoderMessage, EthCallDecoderOutputs, EthCallResult, EventCallResult, OnceCallResult};
