use std::sync::Arc;

use alloy::primitives::{Address, BlockNumber, Bytes, B256, U256};
use alloy::rpc::types::{
    Block, BlockId, BlockNumberOrTag, Filter, Log, Transaction, TransactionReceipt,
};
use async_trait::async_trait;

use crate::rpc::alchemy::{AlchemyClient, SlidingWindowRateLimiter};
use crate::rpc::provider::{RpcClient, RpcClientConfig, RpcError, RpcProvider};

#[derive(Clone)]
pub enum UnifiedRpcClient {
    Standard(RpcClient),
    Alchemy(AlchemyClient),
}

/// Macro to delegate RpcProvider methods to the inner client.
/// Reduces boilerplate by generating the match expression for each method.
macro_rules! delegate_to_inner {
    ($self:expr, $method:ident $(, $arg:expr)*) => {
        match $self {
            Self::Standard(client) => RpcProvider::$method(client $(, $arg)*).await,
            Self::Alchemy(client) => RpcProvider::$method(client $(, $arg)*).await,
        }
    };
}

#[allow(dead_code)]
impl UnifiedRpcClient {
    pub fn from_url(url: &str) -> Result<Self, RpcError> {
        if url.contains("alchemy") {
            Ok(Self::Alchemy(AlchemyClient::from_url(url, 7500)?))
        } else {
            Ok(Self::Standard(RpcClient::from_url(url)?))
        }
    }

    pub fn from_url_with_alchemy_cu(
        url: &str,
        compute_units_per_second: u32,
    ) -> Result<Self, RpcError> {
        if url.contains("alchemy") {
            Ok(Self::Alchemy(AlchemyClient::from_url(
                url,
                compute_units_per_second,
            )?))
        } else {
            Ok(Self::Standard(RpcClient::from_url(url)?))
        }
    }

    /// Convenience wrapper that infers the provider from the URL string.
    ///
    /// Prefer `from_url_with_options` with an explicit `use_alchemy` flag derived
    /// from the `provider` field in config.  This method is kept for contexts where
    /// only a URL is available (e.g. tests, one-off tooling).
    pub fn from_url_with_options_inferred(
        url: &str,
        compute_units_per_second: u32,
        rpc_concurrency: usize,
        max_batch_size: usize,
        shared_limiter: Option<Arc<SlidingWindowRateLimiter>>,
        force_http2: bool,
    ) -> Result<Self, RpcError> {
        let use_alchemy = url.contains("alchemy");
        Self::from_url_with_options(
            url,
            compute_units_per_second,
            rpc_concurrency,
            max_batch_size,
            shared_limiter,
            force_http2,
            use_alchemy,
        )
    }

    /// Create a client with custom options for Alchemy rate limiting.
    ///
    /// # Arguments
    /// * `url` - RPC endpoint URL
    /// * `compute_units_per_second` - CU/s rate limit (e.g., 7500 for Growth tier)
    /// * `rpc_concurrency` - Max concurrent in-flight RPC requests
    /// * `max_batch_size` - Maximum number of requests per JSON-RPC batch
    /// * `shared_limiter` - Optional shared rate limiter for account-level rate limiting
    /// * `force_http2` - When true, build the HTTP transport with HTTP/2 prior knowledge
    ///   (no HTTP/1.1 fallback) and H2 tuning (adaptive window, keep-alive). Applies to
    ///   both Standard and Alchemy clients.
    /// * `use_alchemy` - When true, use `AlchemyClient` with per-method CU costs and a
    ///   sliding-window rate limiter. When false, use the standard `RpcClient`.
    ///   Caller should set this based on the explicit `provider` field in config rather
    ///   than inferring from the URL.
    pub fn from_url_with_options(
        url: &str,
        compute_units_per_second: u32,
        rpc_concurrency: usize,
        max_batch_size: usize,
        shared_limiter: Option<Arc<SlidingWindowRateLimiter>>,
        force_http2: bool,
        use_alchemy: bool,
    ) -> Result<Self, RpcError> {
        if use_alchemy {
            Ok(Self::Alchemy(AlchemyClient::from_url_with_options(
                url,
                compute_units_per_second,
                rpc_concurrency,
                max_batch_size,
                shared_limiter,
                force_http2,
            )?))
        } else {
            let parsed_url =
                url::Url::parse(url).map_err(|e| RpcError::InvalidUrl(e.to_string()))?;
            let config = RpcClientConfig::new(parsed_url)
                .with_batch_size(max_batch_size)
                .with_concurrency(rpc_concurrency)
                .with_force_http2(force_http2);
            let client = if let Some(limiter) = shared_limiter {
                RpcClient::new_with_shared_limiter(config, limiter)?
            } else {
                RpcClient::new(config)?
            };
            Ok(Self::Standard(client))
        }
    }

    // ============================================================
    // Inherent methods that delegate to RpcProvider trait
    // These allow using UnifiedRpcClient without importing the trait
    // ============================================================

    pub async fn get_block_number(&self) -> Result<BlockNumber, RpcError> {
        <Self as RpcProvider>::get_block_number(self).await
    }

    pub async fn get_block(
        &self,
        block_id: BlockId,
        full_transactions: bool,
    ) -> Result<Option<Block>, RpcError> {
        <Self as RpcProvider>::get_block(self, block_id, full_transactions).await
    }

    pub async fn get_block_by_number(
        &self,
        number: BlockNumberOrTag,
        full_transactions: bool,
    ) -> Result<Option<Block>, RpcError> {
        <Self as RpcProvider>::get_block_by_number(self, number, full_transactions).await
    }

    pub async fn get_transaction(&self, hash: B256) -> Result<Option<Transaction>, RpcError> {
        <Self as RpcProvider>::get_transaction(self, hash).await
    }

    pub async fn get_transaction_receipt(
        &self,
        hash: B256,
    ) -> Result<Option<TransactionReceipt>, RpcError> {
        <Self as RpcProvider>::get_transaction_receipt(self, hash).await
    }

    pub async fn get_logs(&self, filter: &Filter) -> Result<Vec<Log>, RpcError> {
        <Self as RpcProvider>::get_logs(self, filter).await
    }

    pub async fn get_balance(
        &self,
        address: Address,
        block: Option<BlockId>,
    ) -> Result<U256, RpcError> {
        <Self as RpcProvider>::get_balance(self, address, block).await
    }

    pub async fn get_code(
        &self,
        address: Address,
        block: Option<BlockId>,
    ) -> Result<Bytes, RpcError> {
        <Self as RpcProvider>::get_code(self, address, block).await
    }

    pub async fn call(
        &self,
        tx: &alloy::rpc::types::TransactionRequest,
        block: Option<BlockId>,
    ) -> Result<Bytes, RpcError> {
        <Self as RpcProvider>::call(self, tx, block).await
    }

    pub async fn get_block_receipts(
        &self,
        method_name: &str,
        block_number: BlockNumberOrTag,
    ) -> Result<Vec<Option<TransactionReceipt>>, RpcError> {
        <Self as RpcProvider>::get_block_receipts(self, method_name, block_number).await
    }

    pub async fn get_blocks_batch(
        &self,
        block_numbers: Vec<BlockNumberOrTag>,
        full_transactions: bool,
    ) -> Result<Vec<Option<Block>>, RpcError> {
        <Self as RpcProvider>::get_blocks_batch(self, block_numbers, full_transactions).await
    }

    pub async fn get_transaction_receipts_batch(
        &self,
        hashes: Vec<B256>,
    ) -> Result<Vec<Option<TransactionReceipt>>, RpcError> {
        <Self as RpcProvider>::get_transaction_receipts_batch(self, hashes).await
    }

    pub async fn get_logs_batch(&self, filters: Vec<Filter>) -> Result<Vec<Vec<Log>>, RpcError> {
        <Self as RpcProvider>::get_logs_batch(self, filters).await
    }

    pub async fn call_batch(
        &self,
        calls: Vec<(alloy::rpc::types::TransactionRequest, BlockId)>,
    ) -> Result<Vec<Result<Bytes, RpcError>>, RpcError> {
        <Self as RpcProvider>::call_batch(self, calls).await
    }

    // ============================================================
    // Methods that are not part of RpcProvider trait
    // ============================================================

    /// Stream blocks as they are fetched, sending each to the provided channel.
    /// Returns a JoinHandle that completes when all blocks are fetched.
    /// Both Standard and Alchemy clients use concurrent fetching.
    pub fn get_blocks_streaming(
        &self,
        block_numbers: Vec<BlockNumberOrTag>,
        full_transactions: bool,
        result_tx: tokio::sync::mpsc::Sender<(BlockNumberOrTag, Result<Option<Block>, RpcError>)>,
    ) -> tokio::task::JoinHandle<()> {
        match self {
            Self::Standard(client) => {
                client.get_blocks_streaming(block_numbers, full_transactions, result_tx)
            }
            Self::Alchemy(client) => {
                client.get_blocks_streaming(block_numbers, full_transactions, result_tx)
            }
        }
    }

    /// Get block receipts concurrently.
    /// Note: The `concurrency` parameter is deprecated for AlchemyClient (uses rpc_concurrency from config).
    pub async fn get_block_receipts_concurrent(
        &self,
        method_name: &str,
        block_numbers: Vec<BlockNumberOrTag>,
        concurrency: usize,
    ) -> Result<Vec<Vec<Option<TransactionReceipt>>>, RpcError> {
        match self {
            Self::Standard(client) => {
                client
                    .get_block_receipts_concurrent(method_name, block_numbers, concurrency)
                    .await
            }
            Self::Alchemy(client) => {
                client
                    .get_block_receipts_concurrent(method_name, block_numbers, concurrency)
                    .await
            }
        }
    }

    /// Execute a single eth_call at a specific block.
    pub async fn eth_call(
        &self,
        to: Address,
        data: Bytes,
        block_number: u64,
    ) -> Result<Bytes, RpcError> {
        let tx = alloy::rpc::types::TransactionRequest::default()
            .to(to)
            .input(alloy::rpc::types::TransactionInput::new(data));
        let block_id = BlockId::Number(BlockNumberOrTag::Number(block_number));

        let results = self.call_batch(vec![(tx, block_id)]).await?;
        results
            .into_iter()
            .next()
            .ok_or_else(|| RpcError::BatchError("Empty batch result".to_string()))?
    }
}

#[async_trait]
impl RpcProvider for UnifiedRpcClient {
    async fn get_block_number(&self) -> Result<BlockNumber, RpcError> {
        delegate_to_inner!(self, get_block_number)
    }

    async fn get_block(
        &self,
        block_id: BlockId,
        full_transactions: bool,
    ) -> Result<Option<Block>, RpcError> {
        delegate_to_inner!(self, get_block, block_id, full_transactions)
    }

    async fn get_transaction(&self, hash: B256) -> Result<Option<Transaction>, RpcError> {
        delegate_to_inner!(self, get_transaction, hash)
    }

    async fn get_transaction_receipt(
        &self,
        hash: B256,
    ) -> Result<Option<TransactionReceipt>, RpcError> {
        delegate_to_inner!(self, get_transaction_receipt, hash)
    }

    async fn get_logs(&self, filter: &Filter) -> Result<Vec<Log>, RpcError> {
        delegate_to_inner!(self, get_logs, filter)
    }

    async fn get_balance(
        &self,
        address: Address,
        block: Option<BlockId>,
    ) -> Result<U256, RpcError> {
        delegate_to_inner!(self, get_balance, address, block)
    }

    async fn get_code(&self, address: Address, block: Option<BlockId>) -> Result<Bytes, RpcError> {
        delegate_to_inner!(self, get_code, address, block)
    }

    async fn call(
        &self,
        tx: &alloy::rpc::types::TransactionRequest,
        block: Option<BlockId>,
    ) -> Result<Bytes, RpcError> {
        delegate_to_inner!(self, call, tx, block)
    }

    async fn get_block_receipts(
        &self,
        method_name: &str,
        block_number: BlockNumberOrTag,
    ) -> Result<Vec<Option<TransactionReceipt>>, RpcError> {
        delegate_to_inner!(self, get_block_receipts, method_name, block_number)
    }

    async fn get_blocks_batch(
        &self,
        block_numbers: Vec<BlockNumberOrTag>,
        full_transactions: bool,
    ) -> Result<Vec<Option<Block>>, RpcError> {
        delegate_to_inner!(self, get_blocks_batch, block_numbers, full_transactions)
    }

    async fn get_transaction_receipts_batch(
        &self,
        hashes: Vec<B256>,
    ) -> Result<Vec<Option<TransactionReceipt>>, RpcError> {
        delegate_to_inner!(self, get_transaction_receipts_batch, hashes)
    }

    async fn get_logs_batch(&self, filters: Vec<Filter>) -> Result<Vec<Vec<Log>>, RpcError> {
        delegate_to_inner!(self, get_logs_batch, filters)
    }

    async fn call_batch(
        &self,
        calls: Vec<(alloy::rpc::types::TransactionRequest, BlockId)>,
    ) -> Result<Vec<Result<Bytes, RpcError>>, RpcError> {
        delegate_to_inner!(self, call_batch, calls)
    }
}

impl std::fmt::Debug for UnifiedRpcClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Standard(client) => f
                .debug_tuple("UnifiedRpcClient::Standard")
                .field(client)
                .finish(),
            Self::Alchemy(client) => f
                .debug_tuple("UnifiedRpcClient::Alchemy")
                .field(client)
                .finish(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Building a Standard (non-Alchemy) client with force_http2=true should
    /// succeed and select the Standard variant.
    #[test]
    fn from_url_with_options_standard_http2() {
        let client = UnifiedRpcClient::from_url_with_options(
            "https://eth.drpc.org",
            7500,
            100,
            100,
            None,
            true,
            false,
        )
        .expect("should build standard HTTP/2 client");
        assert!(matches!(client, UnifiedRpcClient::Standard(_)));
    }

    /// Building an Alchemy client via explicit use_alchemy=true should succeed and
    /// select the Alchemy variant regardless of the URL string.
    #[test]
    fn from_url_with_options_alchemy_http2() {
        let client = UnifiedRpcClient::from_url_with_options(
            "https://eth-mainnet.g.alchemy.com/v2/test",
            7500,
            100,
            100,
            None,
            true,
            true,
        )
        .expect("should build Alchemy HTTP/2 client");
        assert!(matches!(client, UnifiedRpcClient::Alchemy(_)));
    }

    /// Explicit use_alchemy=true works with non-alchemy-domain URLs (proxy case).
    #[test]
    fn from_url_with_options_alchemy_proxied_url() {
        let client = UnifiedRpcClient::from_url_with_options(
            "https://my-proxy.example.com/rpc",
            7500,
            100,
            100,
            None,
            false,
            true,
        )
        .expect("should build Alchemy client via explicit provider flag");
        assert!(matches!(client, UnifiedRpcClient::Alchemy(_)));
    }

    /// Default force_http2=false path still builds a usable Standard client.
    #[test]
    fn from_url_with_options_standard_no_http2() {
        let client = UnifiedRpcClient::from_url_with_options(
            "https://eth.drpc.org",
            7500,
            100,
            100,
            None,
            false,
            false,
        )
        .expect("should build standard client with default transport");
        assert!(matches!(client, UnifiedRpcClient::Standard(_)));
    }
}
