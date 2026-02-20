//! Transformation system for processing decoded blockchain data.
//!
//! This module provides:
//! - Handler traits for processing decoded events and eth_calls
//! - A transformation context with chain info, historical queries, and RPC access
//! - A handler registry for compile-time registration
//! - An engine that orchestrates handler execution and database writes
//!
//! # Architecture
//!
//! ```text
//! Decoded Events/Calls ──► TransformationEngine ──► Handlers ──► DbOperations ──► PostgreSQL
//!                                │
//!                                └─► TransformationContext
//!                                     ├─ Chain info (name, id, block range)
//!                                     ├─ Decoded data (events, calls)
//!                                     ├─ Historical queries (parquet)
//!                                     └─ Ad-hoc RPC calls
//! ```
//!
//! # Example Handler
//!
//! ```ignore
//! use async_trait::async_trait;
//! use doppler_indexer_rs::transformations::{
//!     TransformationHandler, EventHandler, EventTrigger,
//!     TransformationContext, TransformationError,
//! };
//! use doppler_indexer_rs::db::DbOperation;
//!
//! pub struct MySwapHandler;
//!
//! #[async_trait]
//! impl TransformationHandler for MySwapHandler {
//!     fn name(&self) -> &'static str { "MySwapHandler" }
//!
//!     async fn handle(
//!         &self,
//!         ctx: &TransformationContext<'_>,
//!     ) -> Result<Vec<DbOperation>, TransformationError> {
//!         let mut ops = Vec::new();
//!         for event in ctx.events_of_type("MyContract", "Swap") {
//!             // Process event and build database operations
//!         }
//!         Ok(ops)
//!     }
//! }
//!
//! impl EventHandler for MySwapHandler {
//!     fn triggers(&self) -> Vec<EventTrigger> {
//!         vec![EventTrigger::new("MyContract", "Swap(address,uint256,uint256)")]
//!     }
//! }
//! ```

pub mod context;
pub mod engine;
pub mod error;
pub mod eth_call;
pub mod event;
pub mod historical;
pub mod registry;
pub mod traits;
pub mod util;

// Re-exports for convenience
pub use context::{
    DecodedCall, DecodedEvent, DecodedValue, EthCallRequest, HistoricalCallQuery,
    HistoricalEventQuery, TransactionAddresses, TransformationContext,
};
pub use engine::{
    DecodedCallsMessage, DecodedEventsMessage, ExecutionMode, RangeCompleteMessage,
    TransformationEngine,
};
pub use error::TransformationError;
pub use historical::HistoricalDataReader;
pub use registry::{build_registry, TransformationRegistry};
pub use traits::{EthCallHandler, EthCallTrigger, EventHandler, EventTrigger, TransformationHandler};
