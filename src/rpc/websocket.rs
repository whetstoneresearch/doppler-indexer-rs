//! WebSocket client for subscribing to new block headers.
//!
//! Provides `eth_subscribe("newHeads")` subscription with automatic reconnection
//! and exponential backoff.

use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::B256;
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use url::Url;

use super::UnifiedRpcClient;

/// Configuration for WebSocket reconnection behavior.
#[derive(Debug, Clone)]
pub struct ReconnectConfig {
    /// Initial delay before first reconnection attempt.
    pub initial_delay: Duration,
    /// Maximum delay between reconnection attempts.
    pub max_delay: Duration,
    /// Multiplier for exponential backoff.
    pub backoff_multiplier: f64,
    /// Maximum number of reconnection attempts (0 = infinite).
    pub max_attempts: u32,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            backoff_multiplier: 2.0,
            max_attempts: 0, // Infinite retries
        }
    }
}

impl ReconnectConfig {
    /// Calculate delay for a given attempt number (0-indexed).
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        let delay_ms =
            self.initial_delay.as_millis() as f64 * self.backoff_multiplier.powi(attempt as i32);
        let delay = Duration::from_millis(delay_ms as u64);
        std::cmp::min(delay, self.max_delay)
    }
}

/// Events emitted by the WebSocket client.
#[derive(Debug, Clone)]
pub enum WsEvent {
    /// New block header received.
    NewBlock {
        number: u64,
        hash: B256,
        parent_hash: B256,
        timestamp: u64,
    },
    /// WebSocket disconnected.
    Disconnected {
        last_block: Option<u64>,
    },
    /// WebSocket reconnected after a gap. Use HTTP to backfill missed blocks.
    Reconnected {
        missed_from: u64,
        missed_to: u64,
    },
}

/// Block header from eth_subscribe("newHeads").
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockHeader {
    #[serde(deserialize_with = "deserialize_u64_from_hex")]
    pub number: u64,
    pub hash: B256,
    pub parent_hash: B256,
    #[serde(deserialize_with = "deserialize_u64_from_hex")]
    pub timestamp: u64,
}

fn deserialize_u64_from_hex<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    let s = s.strip_prefix("0x").unwrap_or(&s);
    u64::from_str_radix(s, 16).map_err(serde::de::Error::custom)
}

/// JSON-RPC subscription notification.
#[derive(Debug, Deserialize)]
struct SubscriptionNotification {
    params: SubscriptionParams,
}

#[derive(Debug, Deserialize)]
struct SubscriptionParams {
    result: BlockHeader,
}

/// JSON-RPC response for subscription request.
#[derive(Debug, Deserialize)]
struct SubscriptionResponse {
    result: String,
}

#[derive(Debug, Error)]
#[allow(dead_code)]
pub enum WsError {
    #[error("WebSocket connection error: {0}")]
    Connection(String),
    #[error("WebSocket protocol error: {0}")]
    Protocol(String),
    #[error("JSON parsing error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Invalid URL: {0}")]
    InvalidUrl(String),
    #[error("Subscription failed: {0}")]
    SubscriptionFailed(String),
    #[error("Channel closed")]
    ChannelClosed,
}

/// WebSocket client for subscribing to new block headers.
pub struct WsClient {
    url: Url,
    http_client: Arc<UnifiedRpcClient>,
    reconnect_config: ReconnectConfig,
    is_alchemy: bool,
}

#[allow(dead_code)]
impl WsClient {
    /// Create a new WebSocket client.
    pub fn new(
        ws_url: &str,
        http_client: Arc<UnifiedRpcClient>,
        reconnect_config: ReconnectConfig,
    ) -> Result<Self, WsError> {
        let url = Url::parse(ws_url).map_err(|e| WsError::InvalidUrl(e.to_string()))?;
        let is_alchemy = ws_url.contains("alchemy");

        Ok(Self {
            url,
            http_client,
            reconnect_config,
            is_alchemy,
        })
    }

    /// Create a new WebSocket client with default reconnection config.
    pub fn from_urls(
        ws_url: &str,
        http_client: Arc<UnifiedRpcClient>,
    ) -> Result<Self, WsError> {
        Self::new(ws_url, http_client, ReconnectConfig::default())
    }

    /// Whether this client is connected to Alchemy.
    pub fn is_alchemy(&self) -> bool {
        self.is_alchemy
    }

    /// Get the HTTP client for backfill operations.
    pub fn http_client(&self) -> &Arc<UnifiedRpcClient> {
        &self.http_client
    }

    /// Start subscribing to new block headers.
    ///
    /// This spawns a background task that maintains the WebSocket connection,
    /// handles reconnection, and sends events through the provided channel.
    ///
    /// Returns a handle to the background task.
    pub fn subscribe(
        self: Arc<Self>,
        event_tx: mpsc::UnboundedSender<WsEvent>,
    ) -> tokio::task::JoinHandle<Result<(), WsError>> {
        tokio::spawn(async move { self.run_subscription(event_tx).await })
    }

    async fn run_subscription(
        &self,
        event_tx: mpsc::UnboundedSender<WsEvent>,
    ) -> Result<(), WsError> {
        let mut last_block: Option<u64> = None;
        let mut reconnect_attempt = 0u32;

        loop {
            match self.connect_and_subscribe(&event_tx, &mut last_block).await {
                Ok(()) => {
                    // Clean exit (subscription cancelled)
                    tracing::info!("WebSocket subscription ended cleanly");
                    return Ok(());
                }
                Err(e) => {
                    tracing::warn!("WebSocket error: {}", e);

                    // Send disconnection event
                    if event_tx
                        .send(WsEvent::Disconnected { last_block })
                        .is_err()
                    {
                        return Err(WsError::ChannelClosed);
                    }

                    // Check max attempts
                    if self.reconnect_config.max_attempts > 0
                        && reconnect_attempt >= self.reconnect_config.max_attempts
                    {
                        tracing::error!(
                            "WebSocket max reconnection attempts ({}) exceeded",
                            self.reconnect_config.max_attempts
                        );
                        return Err(e);
                    }

                    // Wait before reconnecting
                    let delay = self.reconnect_config.delay_for_attempt(reconnect_attempt);
                    tracing::info!(
                        "Reconnecting in {:?} (attempt {})",
                        delay,
                        reconnect_attempt + 1
                    );
                    tokio::time::sleep(delay).await;

                    reconnect_attempt += 1;
                }
            }
        }
    }

    async fn connect_and_subscribe(
        &self,
        event_tx: &mpsc::UnboundedSender<WsEvent>,
        last_block: &mut Option<u64>,
    ) -> Result<(), WsError> {
        tracing::info!("Connecting to WebSocket at {}", self.url);

        let (ws_stream, _) = connect_async(self.url.as_str())
            .await
            .map_err(|e| WsError::Connection(e.to_string()))?;

        let (mut write, mut read) = ws_stream.split();

        // Subscribe to newHeads
        let subscribe_msg = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_subscribe",
            "params": ["newHeads"]
        });

        write
            .send(Message::Text(subscribe_msg.to_string().into()))
            .await
            .map_err(|e| WsError::Protocol(e.to_string()))?;

        // Wait for subscription confirmation
        let _subscription_id = loop {
            match read.next().await {
                Some(Ok(Message::Text(text))) => {
                    if let Ok(response) = serde_json::from_str::<SubscriptionResponse>(&text) {
                        tracing::info!("Subscribed to newHeads, subscription_id: {}", response.result);
                        break response.result;
                    }
                    // Not a subscription response, might be an error
                    tracing::debug!("Unexpected response during subscription: {}", text);
                }
                Some(Ok(Message::Close(_))) => {
                    return Err(WsError::Connection("Connection closed during subscription".into()));
                }
                Some(Err(e)) => {
                    return Err(WsError::Protocol(e.to_string()));
                }
                None => {
                    return Err(WsError::Connection("Stream ended during subscription".into()));
                }
                _ => continue,
            }
        };

        // Get current block to detect gaps after reconnection
        let _reconnect_block = if let Some(last) = *last_block {
            let current = self
                .http_client
                .get_block_number()
                .await
                .map_err(|e| WsError::Connection(format!("Failed to get current block: {}", e)))?;

            if current > last + 1 {
                // Gap detected, send reconnected event
                tracing::info!(
                    "Detected gap: last_block={}, current_block={}, backfill range [{}, {}]",
                    last,
                    current,
                    last + 1,
                    current
                );
                if event_tx
                    .send(WsEvent::Reconnected {
                        missed_from: last + 1,
                        missed_to: current,
                    })
                    .is_err()
                {
                    return Err(WsError::ChannelClosed);
                }
            }
            Some(current)
        } else {
            None
        };

        // Listen for block headers
        tracing::info!("Listening for new blocks...");
        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    if let Ok(notification) =
                        serde_json::from_str::<SubscriptionNotification>(&text)
                    {
                        let header = notification.params.result;
                        tracing::debug!(
                            "New block: number={}, hash={:?}",
                            header.number,
                            header.hash
                        );

                        *last_block = Some(header.number);

                        if event_tx
                            .send(WsEvent::NewBlock {
                                number: header.number,
                                hash: header.hash,
                                parent_hash: header.parent_hash,
                                timestamp: header.timestamp,
                            })
                            .is_err()
                        {
                            return Err(WsError::ChannelClosed);
                        }
                    }
                }
                Ok(Message::Ping(data)) => {
                    let _ = write.send(Message::Pong(data)).await;
                }
                Ok(Message::Close(_)) => {
                    tracing::info!("WebSocket closed by server");
                    return Err(WsError::Connection("Connection closed by server".into()));
                }
                Err(e) => {
                    return Err(WsError::Protocol(e.to_string()));
                }
                _ => {}
            }
        }

        Ok(())
    }
}

impl std::fmt::Debug for WsClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WsClient")
            .field("url", &self.url.to_string())
            .field("is_alchemy", &self.is_alchemy)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reconnect_config_delay() {
        let config = ReconnectConfig::default();

        assert_eq!(config.delay_for_attempt(0), Duration::from_millis(100));
        assert_eq!(config.delay_for_attempt(1), Duration::from_millis(200));
        assert_eq!(config.delay_for_attempt(2), Duration::from_millis(400));

        // Should cap at max_delay
        let large_delay = config.delay_for_attempt(20);
        assert_eq!(large_delay, config.max_delay);
    }

    #[test]
    fn test_deserialize_block_header() {
        let json = r#"{
            "number": "0x1234",
            "hash": "0x0000000000000000000000000000000000000000000000000000000000001234",
            "parentHash": "0x0000000000000000000000000000000000000000000000000000000000001233",
            "timestamp": "0x5678"
        }"#;

        let header: BlockHeader = serde_json::from_str(json).unwrap();
        assert_eq!(header.number, 0x1234);
        assert_eq!(header.timestamp, 0x5678);
    }
}
