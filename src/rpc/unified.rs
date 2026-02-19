use std::sync::Arc;

use alloy::primitives::{Address, BlockNumber, Bytes, B256, U256};
use alloy::providers::Provider;
use alloy::rpc::types::{
    Block, BlockId, BlockNumberOrTag, Filter, Log, Transaction, TransactionReceipt,
};
use async_trait::async_trait;

use crate::rpc::alchemy::{AlchemyClient, SlidingWindowRateLimiter};
use crate::rpc::rpc::{RpcClient, RpcError, RpcProvider};

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

    /// Create a client with custom options for Alchemy rate limiting.
    ///
    /// # Arguments
    /// * `url` - RPC endpoint URL
    /// * `compute_units_per_second` - CU/s rate limit (e.g., 7500 for Growth tier)
    /// * `rpc_concurrency` - Max concurrent in-flight RPC requests
    /// * `shared_limiter` - Optional shared rate limiter for account-level rate limiting
    pub fn from_url_with_options(
        url: &str,
        compute_units_per_second: u32,
        rpc_concurrency: usize,
        shared_limiter: Option<Arc<SlidingWindowRateLimiter>>,
    ) -> Result<Self, RpcError> {
        if url.contains("alchemy") {
            Ok(Self::Alchemy(AlchemyClient::from_url_with_options(
                url,
                compute_units_per_second,
                rpc_concurrency,
                shared_limiter,
            )?))
        } else {
            Ok(Self::Standard(RpcClient::from_url(url)?))
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
    /// For Standard client, falls back to sequential fetching.
    pub fn get_blocks_streaming(
        &self,
        block_numbers: Vec<BlockNumberOrTag>,
        full_transactions: bool,
        result_tx: tokio::sync::mpsc::Sender<(BlockNumberOrTag, Result<Option<Block>, RpcError>)>,
    ) -> tokio::task::JoinHandle<()> {
        match self {
            Self::Standard(client) => {
                // Fallback: fetch sequentially and send to channel
                let provider = client.provider().clone();
                tokio::spawn(async move {
                    for number in block_numbers {
                        let result = async {
                            let builder = provider.get_block(BlockId::Number(number));
                            if full_transactions {
                                builder.full().await
                            } else {
                                builder.await
                            }
                        }
                        .await
                        .map_err(|e| RpcError::ProviderError(format!("{:?}", e)));
                        let _ = result_tx.send((number, result)).await;
                    }
                })
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

    async fn get_balance(&self, address: Address, block: Option<BlockId>) -> Result<U256, RpcError> {
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
