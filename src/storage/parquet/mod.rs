//! Shared parquet readers for logs, eth_calls, factory addresses, and schema inspection.
//!
//! These functions were previously duplicated across `decoding/catchup/`,
//! `raw_data/historical/eth_calls/`, and `raw_data/historical/factories.rs`.
//! Centralising them here avoids drift and makes testing easier.

mod error;
pub mod eth_calls;
pub mod factories;
pub mod logs;
pub mod once_calls;
pub mod schema;

pub use error::ParquetReadError;
