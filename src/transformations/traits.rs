//! Core traits for transformation handlers.
//!
//! Handlers implement these traits to receive decoded events and eth_calls,
//! transform the data, and produce database operations.

use async_trait::async_trait;

use crate::db::{DbOperation, DbPool};

use super::context::TransformationContext;
use super::error::TransformationError;

/// Core trait that all transformation handlers must implement.
///
/// Handlers are registered at compile-time and invoked when their
/// registered triggers (events or calls) are decoded.
#[async_trait]
pub trait TransformationHandler: Send + Sync + 'static {
    /// Unique name for this handler (used in logging and error messages).
    fn name(&self) -> &'static str;

    /// Process decoded data for a block range.
    ///
    /// Called once per block range with all decoded events/calls matching
    /// this handler's triggers. Returns a list of database operations to
    /// execute transactionally.
    async fn handle(
        &self,
        ctx: &TransformationContext<'_>,
    ) -> Result<Vec<DbOperation>, TransformationError>;

    /// Optional: Called once at startup for initialization.
    ///
    /// Can be used to create indexes, warm caches, etc.
    #[allow(unused_variables)]
    async fn initialize(&self, db_pool: &DbPool) -> Result<(), TransformationError> {
        Ok(())
    }
}

/// Trigger for event-based handlers.
#[derive(Debug, Clone)]
pub struct EventTrigger {
    /// Contract name or factory collection name from config.
    pub source: String,
    /// Event signature (e.g., "Swap(bytes32,address,int128,int128,uint160,uint128,int24,uint24)").
    pub event_signature: String,
}

impl EventTrigger {
    pub fn new(source: impl Into<String>, event_signature: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            event_signature: event_signature.into(),
        }
    }
}

/// Trigger for eth_call-based handlers.
#[derive(Debug, Clone)]
pub struct EthCallTrigger {
    /// Contract name or factory collection name from config.
    pub source: String,
    /// Function name (e.g., "slot0").
    pub function_name: String,
}

impl EthCallTrigger {
    pub fn new(source: impl Into<String>, function_name: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            function_name: function_name.into(),
        }
    }
}

/// Marker trait for handlers that respond to events.
pub trait EventHandler: TransformationHandler {
    /// Event triggers this handler responds to.
    fn triggers(&self) -> Vec<EventTrigger>;
}

/// Marker trait for handlers that respond to eth_call results.
pub trait EthCallHandler: TransformationHandler {
    /// eth_call triggers this handler responds to.
    fn triggers(&self) -> Vec<EthCallTrigger>;
}
