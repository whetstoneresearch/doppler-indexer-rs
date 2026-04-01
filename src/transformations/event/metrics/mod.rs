//! Shared metrics types and processing logic for pool metrics handlers.
//!
//! All pool type handlers normalize events into SwapInput/LiquidityInput,
//! then delegate to the shared process_swaps()/process_liquidity_deltas() functions.

pub mod accumulator;
pub mod swap_data;
