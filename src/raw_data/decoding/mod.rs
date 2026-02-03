mod eth_calls;
mod event_parsing;
mod logs;
mod types;

pub use eth_calls::decode_eth_calls;
pub use logs::decode_logs;
pub use types::{DecoderMessage, EventCallResult};
