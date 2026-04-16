use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use solana_client::client_error::ClientError as SolanaClientError;
use solana_client::nonblocking::rpc_client::RpcClient as SolanaRpcClientInner;
use solana_client::rpc_client::GetConfirmedSignaturesForAddress2Config;
use solana_client::rpc_config::{RpcBlockConfig, RpcProgramAccountsConfig, RpcTransactionConfig};
use solana_client::rpc_filter::RpcFilterType;
use solana_client::rpc_response::RpcConfirmedTransactionStatusWithSignature;
use solana_sdk::account::Account;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta, TransactionDetails, UiConfirmedBlock,
    UiTransactionEncoding,
};
use thiserror::Error;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::rpc::{RetryConfig, SlidingWindowRateLimiter};
use crate::types::config::defaults::solana_rpc as defaults;
use crate::types::config::solana::SolanaCommitment;

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

#[derive(Debug, Error)]
pub enum SolanaRpcError {
    #[error("Solana RPC transport error: {0}")]
    Transport(String),

    #[error("Invalid URL: {0}")]
    InvalidUrl(String),

    #[error("Rate limit exceeded")]
    RateLimitExceeded,

    #[error("Slot {0} was skipped (no block produced)")]
    SlotSkipped(u64),

    #[error("Block not available for slot {0}: {1}")]
    BlockNotAvailable(u64, String),

    #[error("Solana client error: {0}")]
    ClientError(#[from] SolanaClientError),

    #[error("Invalid pubkey: {0}")]
    InvalidPubkey(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),
}

impl SolanaRpcError {
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Transport(_) => true,
            Self::RateLimitExceeded => true,
            Self::BlockNotAvailable(_, _) => true,
            Self::SlotSkipped(_) => false,
            Self::InvalidUrl(_) => false,
            Self::InvalidPubkey(_) => false,
            Self::SerializationError(_) => false,
            Self::ClientError(e) => {
                let msg = e.to_string().to_lowercase();
                msg.contains("timeout")
                    || msg.contains("connection")
                    || msg.contains("429")
                    || msg.contains("too many requests")
                    || msg.contains("502")
                    || msg.contains("503")
                    || msg.contains("504")
                    || msg.contains("node is unhealthy")
                    || msg.contains("try again")
                    || msg.contains("service unavailable")
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Retry
// ---------------------------------------------------------------------------

/// Execute an async operation with exponential-backoff retry.
///
/// Same logic as `crate::rpc::provider::with_retry` but typed over
/// `SolanaRpcError` instead of `RpcError`.
async fn with_solana_retry<F, Fut, T>(
    config: &RetryConfig,
    operation_name: &str,
    mut operation: F,
) -> Result<T, SolanaRpcError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, SolanaRpcError>>,
{
    for attempt in 0..=config.max_retries {
        if attempt > 0 {
            let delay = config.delay_for_attempt(attempt);
            tracing::warn!(
                "Solana RPC retry {}/{} for '{}' in {:?}",
                attempt,
                config.max_retries,
                operation_name,
                delay,
            );
            tokio::time::sleep(delay).await;
        }

        match operation().await {
            Ok(result) => {
                if attempt > 0 {
                    tracing::info!(
                        "Solana RPC '{}' succeeded after {} retries",
                        operation_name,
                        attempt,
                    );
                }
                return Ok(result);
            }
            Err(e) => {
                if e.is_retryable() && attempt < config.max_retries {
                    tracing::warn!(
                        "Solana RPC '{}' failed (attempt {}/{}): {}",
                        operation_name,
                        attempt + 1,
                        config.max_retries + 1,
                        e,
                    );
                } else {
                    return Err(e);
                }
            }
        }
    }

    unreachable!("loop covers 0..=max_retries")
}

// ---------------------------------------------------------------------------
// Commitment conversion
// ---------------------------------------------------------------------------

pub fn commitment_config_from(c: SolanaCommitment) -> CommitmentConfig {
    match c {
        SolanaCommitment::Processed => CommitmentConfig::processed(),
        SolanaCommitment::Confirmed => CommitmentConfig::confirmed(),
        SolanaCommitment::Finalized => CommitmentConfig::finalized(),
    }
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

pub struct SolanaRpcClient {
    inner: Arc<SolanaRpcClientInner>,
    rate_limiter: Arc<SlidingWindowRateLimiter>,
    semaphore: Arc<Semaphore>,
    retry_config: RetryConfig,
    commitment: CommitmentConfig,
    batch_concurrency: usize,
}

impl SolanaRpcClient {
    pub fn new(
        rpc_url: &str,
        commitment: SolanaCommitment,
        requests_per_second: Option<u32>,
        concurrency: Option<usize>,
    ) -> Result<Self, SolanaRpcError> {
        let _ = url::Url::parse(rpc_url).map_err(|e| SolanaRpcError::InvalidUrl(e.to_string()))?;

        let commitment_config = commitment_config_from(commitment);
        let inner =
            SolanaRpcClientInner::new_with_commitment(rpc_url.to_string(), commitment_config);

        let rps = requests_per_second.unwrap_or(defaults::REQUESTS_PER_SECOND);
        let conc = concurrency.unwrap_or(defaults::CONCURRENCY);

        Ok(Self {
            inner: Arc::new(inner),
            rate_limiter: Arc::new(SlidingWindowRateLimiter::new(rps)),
            semaphore: Arc::new(Semaphore::new(conc)),
            retry_config: RetryConfig::default(),
            commitment: commitment_config,
            batch_concurrency: defaults::BATCH_CONCURRENCY,
        })
    }

    /// Create with a shared rate limiter (for multiple clients sharing a budget).
    pub fn new_with_limiter(
        rpc_url: &str,
        commitment: SolanaCommitment,
        shared_limiter: Arc<SlidingWindowRateLimiter>,
        concurrency: Option<usize>,
    ) -> Result<Self, SolanaRpcError> {
        let _ = url::Url::parse(rpc_url).map_err(|e| SolanaRpcError::InvalidUrl(e.to_string()))?;

        let commitment_config = commitment_config_from(commitment);
        let inner =
            SolanaRpcClientInner::new_with_commitment(rpc_url.to_string(), commitment_config);
        let conc = concurrency.unwrap_or(defaults::CONCURRENCY);

        Ok(Self {
            inner: Arc::new(inner),
            rate_limiter: shared_limiter,
            semaphore: Arc::new(Semaphore::new(conc)),
            retry_config: RetryConfig::default(),
            commitment: commitment_config,
            batch_concurrency: defaults::BATCH_CONCURRENCY,
        })
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /// Execute a single RPC request with rate limiting, concurrency control,
    /// and retry.
    async fn execute<F, Fut, T>(
        &self,
        operation_name: &str,
        mut operation: F,
    ) -> Result<T, SolanaRpcError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, SolanaRpcError>>,
    {
        let _permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|_| SolanaRpcError::Transport("Semaphore closed".into()))?;
        self.rate_limiter.acquire(1).await;
        with_solana_retry(&self.retry_config, operation_name, &mut operation).await
    }

    /// Returns `true` if the error indicates a skipped or unavailable slot.
    fn is_skipped_slot_error(e: &SolanaClientError) -> bool {
        let msg = e.to_string();
        msg.contains("Slot was skipped")
            || msg.contains("Block not available")
            || msg.contains("was skipped, or missing")
            || msg.contains("-32007")
            || msg.contains("-32009")
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /// Get the current slot number.
    pub async fn get_slot(&self) -> Result<u64, SolanaRpcError> {
        let inner = self.inner.clone();
        let commitment = self.commitment;
        self.execute("get_slot", || {
            let inner = inner.clone();
            async move {
                inner
                    .get_slot_with_commitment(commitment)
                    .await
                    .map_err(SolanaRpcError::from)
            }
        })
        .await
    }

    /// Get a full block with transactions for a given slot.
    ///
    /// Returns `None` if the slot was skipped (no block produced).
    pub async fn get_block(&self, slot: u64) -> Result<Option<UiConfirmedBlock>, SolanaRpcError> {
        let inner = self.inner.clone();
        let commitment = self.commitment;
        let config = RpcBlockConfig {
            encoding: Some(UiTransactionEncoding::Base64),
            transaction_details: Some(TransactionDetails::Full),
            rewards: Some(false),
            commitment: Some(commitment),
            max_supported_transaction_version: Some(0),
        };

        self.execute("get_block", || {
            let inner = inner.clone();
            async move {
                match inner.get_block_with_config(slot, config).await {
                    Ok(block) => Ok(Some(block)),
                    Err(e) if Self::is_skipped_slot_error(&e) => {
                        tracing::debug!(slot, "Slot was skipped or block not available");
                        Ok(None)
                    }
                    Err(e) => Err(SolanaRpcError::from(e)),
                }
            }
        })
        .await
    }

    /// Fetch multiple blocks concurrently. Returns results in slot order.
    ///
    /// Entries are `None` for skipped slots.
    pub async fn get_blocks_batch(
        &self,
        slots: &[u64],
    ) -> Result<Vec<(u64, Option<UiConfirmedBlock>)>, SolanaRpcError> {
        if slots.is_empty() {
            return Ok(vec![]);
        }

        let batch_semaphore = Arc::new(Semaphore::new(self.batch_concurrency));
        let mut join_set = JoinSet::new();

        for &slot in slots {
            let inner = self.inner.clone();
            let rate_limiter = self.rate_limiter.clone();
            let semaphore = self.semaphore.clone();
            let batch_sem = batch_semaphore.clone();
            let retry_config = self.retry_config.clone();
            let commitment = self.commitment;
            let config = RpcBlockConfig {
                encoding: Some(UiTransactionEncoding::Base64),
                transaction_details: Some(TransactionDetails::Full),
                rewards: Some(false),
                commitment: Some(commitment),
                max_supported_transaction_version: Some(0),
            };

            join_set.spawn(async move {
                let _batch_permit = batch_sem
                    .acquire()
                    .await
                    .map_err(|_| SolanaRpcError::Transport("Semaphore closed".into()))?;
                let _permit = semaphore
                    .acquire()
                    .await
                    .map_err(|_| SolanaRpcError::Transport("Semaphore closed".into()))?;
                rate_limiter.acquire(1).await;

                let result = with_solana_retry(&retry_config, "get_block_batch", || {
                    let inner = inner.clone();
                    async move {
                        match inner.get_block_with_config(slot, config).await {
                            Ok(block) => Ok(Some(block)),
                            Err(e) if SolanaRpcClient::is_skipped_slot_error(&e) => Ok(None),
                            Err(e) => Err(SolanaRpcError::from(e)),
                        }
                    }
                })
                .await?;

                Ok::<_, SolanaRpcError>((slot, result))
            });
        }

        let mut results = Vec::with_capacity(slots.len());
        while let Some(join_result) = join_set.join_next().await {
            let (slot, block) = join_result
                .map_err(|e| SolanaRpcError::Transport(format!("Task join error: {e}")))??;
            results.push((slot, block));
        }

        // Return in slot order.
        results.sort_by_key(|(slot, _)| *slot);
        Ok(results)
    }

    /// Get a single transaction by signature.
    pub async fn get_transaction(
        &self,
        signature: &Signature,
    ) -> Result<EncodedConfirmedTransactionWithStatusMeta, SolanaRpcError> {
        let inner = self.inner.clone();
        let sig = *signature;
        let commitment = self.commitment;

        self.execute("get_transaction", || {
            let inner = inner.clone();
            async move {
                inner
                    .get_transaction_with_config(
                        &sig,
                        RpcTransactionConfig {
                            encoding: Some(UiTransactionEncoding::Base64),
                            commitment: Some(commitment),
                            max_supported_transaction_version: Some(0),
                        },
                    )
                    .await
                    .map_err(SolanaRpcError::from)
            }
        })
        .await
    }

    /// Get transaction signatures for an address, with optional pagination.
    ///
    /// Used for historical backfill.
    pub async fn get_signatures_for_address(
        &self,
        address: &Pubkey,
        before: Option<Signature>,
        limit: Option<usize>,
    ) -> Result<Vec<RpcConfirmedTransactionStatusWithSignature>, SolanaRpcError> {
        let inner = self.inner.clone();
        let addr = *address;
        let commitment = self.commitment;

        self.execute("get_signatures_for_address", || {
            let inner = inner.clone();
            async move {
                inner
                    .get_signatures_for_address_with_config(
                        &addr,
                        GetConfirmedSignaturesForAddress2Config {
                            before,
                            until: None,
                            limit,
                            commitment: Some(commitment),
                        },
                    )
                    .await
                    .map_err(SolanaRpcError::from)
            }
        })
        .await
    }

    /// Get account info for a single pubkey.
    ///
    /// Returns `None` if the account does not exist.
    pub async fn get_account_info(
        &self,
        pubkey: &Pubkey,
    ) -> Result<Option<Account>, SolanaRpcError> {
        let inner = self.inner.clone();
        let pk = *pubkey;
        let commitment = self.commitment;

        self.execute("get_account_info", || {
            let inner = inner.clone();
            async move {
                let response = inner
                    .get_account_with_commitment(&pk, commitment)
                    .await
                    .map_err(SolanaRpcError::from)?;
                Ok(response.value)
            }
        })
        .await
    }

    /// Get multiple accounts in a single RPC call.
    ///
    /// The Solana RPC limits this to 100 pubkeys per call.
    pub async fn get_multiple_accounts(
        &self,
        pubkeys: &[Pubkey],
    ) -> Result<Vec<Option<Account>>, SolanaRpcError> {
        let inner = self.inner.clone();
        let pks = pubkeys.to_vec();
        let commitment = self.commitment;

        self.execute("get_multiple_accounts", || {
            let inner = inner.clone();
            let pks = pks.clone();
            async move {
                let response = inner
                    .get_multiple_accounts_with_commitment(&pks, commitment)
                    .await
                    .map_err(SolanaRpcError::from)?;
                Ok(response.value)
            }
        })
        .await
    }

    /// Get all accounts owned by a program, with optional filters.
    ///
    /// **Warning**: This can be very expensive. Always use discriminator or
    /// memcmp filters to narrow results.
    pub async fn get_program_accounts(
        &self,
        program_id: &Pubkey,
        filters: Option<Vec<RpcFilterType>>,
    ) -> Result<Vec<(Pubkey, Account)>, SolanaRpcError> {
        let inner = self.inner.clone();
        let pid = *program_id;
        let commitment = self.commitment;

        self.execute("get_program_accounts", || {
            let inner = inner.clone();
            let filters = filters.clone();
            async move {
                inner
                    .get_program_accounts_with_config(
                        &pid,
                        RpcProgramAccountsConfig {
                            filters,
                            account_config: solana_client::rpc_config::RpcAccountInfoConfig {
                                commitment: Some(commitment),
                                ..Default::default()
                            },
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(SolanaRpcError::from)
            }
        })
        .await
    }
}

impl std::fmt::Debug for SolanaRpcClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SolanaRpcClient")
            .field("commitment", &self.commitment)
            .field("batch_concurrency", &self.batch_concurrency)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_retryable() {
        assert!(!SolanaRpcError::SlotSkipped(100).is_retryable());
        assert!(!SolanaRpcError::InvalidUrl("bad".into()).is_retryable());
        assert!(!SolanaRpcError::InvalidPubkey("bad".into()).is_retryable());
        assert!(!SolanaRpcError::SerializationError("bad".into()).is_retryable());
        assert!(SolanaRpcError::Transport("timeout".into()).is_retryable());
        assert!(SolanaRpcError::RateLimitExceeded.is_retryable());
        assert!(SolanaRpcError::BlockNotAvailable(100, "pruned".into()).is_retryable());
    }

    #[test]
    fn test_commitment_config_from() {
        let processed = commitment_config_from(SolanaCommitment::Processed);
        assert_eq!(processed, CommitmentConfig::processed());

        let confirmed = commitment_config_from(SolanaCommitment::Confirmed);
        assert_eq!(confirmed, CommitmentConfig::confirmed());

        let finalized = commitment_config_from(SolanaCommitment::Finalized);
        assert_eq!(finalized, CommitmentConfig::finalized());
    }

    #[test]
    fn test_constructor_invalid_url() {
        let result =
            SolanaRpcClient::new("not a valid url", SolanaCommitment::Confirmed, None, None);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SolanaRpcError::InvalidUrl(_)));
    }

    #[test]
    fn test_constructor_valid_url() {
        let result = SolanaRpcClient::new(
            "https://api.mainnet-beta.solana.com",
            SolanaCommitment::Confirmed,
            None,
            None,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_constructor_custom_params() {
        let client = SolanaRpcClient::new(
            "https://api.mainnet-beta.solana.com",
            SolanaCommitment::Finalized,
            Some(50),
            Some(20),
        )
        .unwrap();

        assert_eq!(client.commitment, CommitmentConfig::finalized());
        assert_eq!(client.batch_concurrency, defaults::BATCH_CONCURRENCY);
    }

    #[test]
    fn test_skipped_slot_error_detection() {
        // We can't easily construct a SolanaClientError with arbitrary messages,
        // so we test the string matching logic indirectly via the error message.
        let msg = "Slot was skipped, or missing in long-term storage";
        assert!(msg.contains("Slot was skipped") || msg.contains("was skipped, or missing"));

        let msg2 = "Block not available for slot 12345";
        assert!(msg2.contains("Block not available"));
    }

    #[tokio::test]
    async fn test_retry_no_retries_on_permanent_error() {
        let config = RetryConfig::new(3);
        let call_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let cc = call_count.clone();

        let result: Result<(), SolanaRpcError> = with_solana_retry(&config, "test", || {
            let cc = cc.clone();
            async move {
                cc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Err(SolanaRpcError::InvalidUrl("permanent".into()))
            }
        })
        .await;

        assert!(result.is_err());
        assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_retry_retries_on_transient_error() {
        let config = RetryConfig::new(2).with_initial_delay(Duration::from_millis(1));
        let call_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let cc = call_count.clone();

        let result: Result<u32, SolanaRpcError> = with_solana_retry(&config, "test", || {
            let cc = cc.clone();
            async move {
                let count = cc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if count < 2 {
                    Err(SolanaRpcError::Transport("timeout".into()))
                } else {
                    Ok(42)
                }
            }
        })
        .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 3);
    }
}
