use std::sync::Arc;

use futures::StreamExt;
use solana_client::nonblocking::pubsub_client::PubsubClient;
use thiserror::Error;
use tokio::sync::mpsc;

use crate::rpc::ReconnectConfig;

use super::rpc::{SolanaRpcClient, SolanaRpcError};

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

/// Events emitted by the Solana WebSocket client.
#[derive(Debug, Clone)]
pub enum SolanaWsEvent {
    /// New slot notification from `slotSubscribe`.
    NewSlot { slot: u64, parent: u64, root: u64 },
    /// WebSocket disconnected.
    Disconnected { last_slot: Option<u64> },
    /// Reconnected after a gap. Caller should backfill the missed slot range.
    Reconnected { missed_from: u64, missed_to: u64 },
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, Error)]
pub enum SolanaWsError {
    #[error("WebSocket connection error: {0}")]
    Connection(String),

    #[error("Subscription failed: {0}")]
    SubscriptionFailed(String),

    #[error("Stream ended unexpectedly")]
    StreamEnded,

    #[error("Channel closed")]
    ChannelClosed,

    #[error("RPC error during gap detection: {0}")]
    RpcError(#[from] SolanaRpcError),

    #[error("PubsubClient error: {0}")]
    PubsubError(String),
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

pub struct SolanaWsClient {
    ws_url: String,
    rpc_client: Arc<SolanaRpcClient>,
    reconnect_config: ReconnectConfig,
}

impl SolanaWsClient {
    pub fn new(
        ws_url: &str,
        rpc_client: Arc<SolanaRpcClient>,
        reconnect_config: ReconnectConfig,
    ) -> Self {
        Self {
            ws_url: ws_url.to_string(),
            rpc_client,
            reconnect_config,
        }
    }

    pub fn with_default_config(
        ws_url: &str,
        rpc_client: Arc<SolanaRpcClient>,
    ) -> Self {
        Self::new(ws_url, rpc_client, ReconnectConfig::default())
    }

    /// Start subscribing to slot notifications.
    ///
    /// Spawns a background task that maintains the WebSocket connection with
    /// automatic reconnection and gap detection. Returns a handle to the task.
    pub fn subscribe(
        self: Arc<Self>,
        event_tx: mpsc::UnboundedSender<SolanaWsEvent>,
    ) -> tokio::task::JoinHandle<Result<(), SolanaWsError>> {
        tokio::spawn(async move { self.run_subscription(event_tx).await })
    }

    // -----------------------------------------------------------------------
    // Subscription loop
    // -----------------------------------------------------------------------

    async fn run_subscription(
        &self,
        event_tx: mpsc::UnboundedSender<SolanaWsEvent>,
    ) -> Result<(), SolanaWsError> {
        let mut last_slot: Option<u64> = None;
        let mut reconnect_attempt = 0u32;

        loop {
            match self
                .connect_and_listen(&event_tx, &mut last_slot)
                .await
            {
                Ok(()) => {
                    tracing::info!("Solana WebSocket subscription ended cleanly");
                    return Ok(());
                }
                Err(e) => {
                    tracing::warn!("Solana WebSocket error: {e}");

                    if event_tx
                        .send(SolanaWsEvent::Disconnected { last_slot })
                        .is_err()
                    {
                        return Err(SolanaWsError::ChannelClosed);
                    }

                    if self.reconnect_config.max_attempts > 0
                        && reconnect_attempt >= self.reconnect_config.max_attempts
                    {
                        tracing::error!(
                            "Solana WebSocket max reconnection attempts ({}) exceeded",
                            self.reconnect_config.max_attempts,
                        );
                        return Err(e);
                    }

                    let delay =
                        self.reconnect_config.delay_for_attempt(reconnect_attempt);
                    tracing::info!(
                        "Solana WS reconnecting in {delay:?} (attempt {})",
                        reconnect_attempt + 1,
                    );
                    tokio::time::sleep(delay).await;
                    reconnect_attempt += 1;
                }
            }
        }
    }

    async fn connect_and_listen(
        &self,
        event_tx: &mpsc::UnboundedSender<SolanaWsEvent>,
        last_slot: &mut Option<u64>,
    ) -> Result<(), SolanaWsError> {
        tracing::info!("Connecting to Solana WebSocket at {}", self.ws_url);

        let pubsub = PubsubClient::new(&self.ws_url)
            .await
            .map_err(|e| SolanaWsError::Connection(e.to_string()))?;

        let (mut slot_stream, _unsubscribe) = pubsub
            .slot_subscribe()
            .await
            .map_err(|e| SolanaWsError::SubscriptionFailed(e.to_string()))?;

        tracing::info!("Subscribed to Solana slot notifications");

        // Gap detection on reconnect.
        if let Some(last) = *last_slot {
            match self.rpc_client.get_slot().await {
                Ok(current) => {
                    if current > last + 1 {
                        tracing::info!(
                            "Solana WS gap detected: last_slot={last}, current_slot={current}, \
                             backfill [{}, {current}]",
                            last + 1,
                        );
                        if event_tx
                            .send(SolanaWsEvent::Reconnected {
                                missed_from: last + 1,
                                missed_to: current,
                            })
                            .is_err()
                        {
                            return Err(SolanaWsError::ChannelClosed);
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to get current slot for gap detection: {e}"
                    );
                }
            }
        }

        // Listen for slot notifications.
        while let Some(slot_info) = slot_stream.next().await {
            tracing::debug!(
                slot = slot_info.slot,
                parent = slot_info.parent,
                root = slot_info.root,
                "Solana slot notification",
            );

            *last_slot = Some(slot_info.slot);

            if event_tx
                .send(SolanaWsEvent::NewSlot {
                    slot: slot_info.slot,
                    parent: slot_info.parent,
                    root: slot_info.root,
                })
                .is_err()
            {
                return Err(SolanaWsError::ChannelClosed);
            }
        }

        Err(SolanaWsError::StreamEnded)
    }
}

impl std::fmt::Debug for SolanaWsClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SolanaWsClient")
            .field("ws_url", &self.ws_url)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::config::solana::SolanaCommitment;

    #[test]
    fn test_event_variants() {
        let new_slot = SolanaWsEvent::NewSlot {
            slot: 100,
            parent: 99,
            root: 90,
        };
        let disconnected = SolanaWsEvent::Disconnected {
            last_slot: Some(100),
        };
        let reconnected = SolanaWsEvent::Reconnected {
            missed_from: 101,
            missed_to: 110,
        };

        // Verify Debug is implemented.
        let _ = format!("{new_slot:?}");
        let _ = format!("{disconnected:?}");
        let _ = format!("{reconnected:?}");

        // Verify Clone is implemented.
        let _cloned = new_slot.clone();
    }

    #[test]
    fn test_constructor() {
        let rpc_client = Arc::new(
            SolanaRpcClient::new(
                "https://api.mainnet-beta.solana.com",
                SolanaCommitment::Confirmed,
                None,
                None,
            )
            .unwrap(),
        );

        let ws = SolanaWsClient::with_default_config(
            "wss://api.mainnet-beta.solana.com",
            rpc_client,
        );

        assert_eq!(ws.ws_url, "wss://api.mainnet-beta.solana.com");
        let _ = format!("{ws:?}");
    }

    #[test]
    fn test_error_variants() {
        let err = SolanaWsError::Connection("test".into());
        assert!(err.to_string().contains("test"));

        let err = SolanaWsError::StreamEnded;
        assert!(err.to_string().contains("unexpectedly"));

        let err = SolanaWsError::ChannelClosed;
        assert!(err.to_string().contains("closed"));
    }
}
