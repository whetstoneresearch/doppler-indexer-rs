use alloy::primitives::{BlockNumber, Bytes, B256};
use alloy::rpc::types::{Block, BlockId, BlockNumberOrTag, TransactionReceipt};

use crate::rpc::alchemy::AlchemyClient;
use crate::rpc::rpc::{RpcClient, RpcError};

pub enum UnifiedRpcClient {
    Standard(RpcClient),
    Alchemy(AlchemyClient),
}

impl UnifiedRpcClient {    
    pub fn from_url(url: &str) -> Result<Self, RpcError> {
        if url.contains("alchemy") {
            Ok(Self::Alchemy(AlchemyClient::from_url(url, 500)?))
        } else {
            Ok(Self::Standard(RpcClient::from_url(url)?))
        }
    }

    pub fn from_url_with_alchemy_cu(url: &str, compute_units_per_second: u32) -> Result<Self, RpcError> {
        if url.contains("alchemy") {
            Ok(Self::Alchemy(AlchemyClient::from_url(url, compute_units_per_second)?))
        } else {
            Ok(Self::Standard(RpcClient::from_url(url)?))
        }
    }

    pub async fn get_block_number(&self) -> Result<BlockNumber, RpcError> {
        match self {
            Self::Standard(client) => client.get_block_number().await,
            Self::Alchemy(client) => client.get_block_number().await,
        }
    }

    pub async fn get_blocks_batch(
        &self,
        block_numbers: Vec<BlockNumberOrTag>,
        full_transactions: bool,
    ) -> Result<Vec<Option<Block>>, RpcError> {
        match self {
            Self::Standard(client) => client.get_blocks_batch(block_numbers, full_transactions).await,
            Self::Alchemy(client) => client.get_blocks_batch(block_numbers, full_transactions).await,
        }
    }

    pub async fn get_transaction_receipts_batch(
        &self,
        hashes: Vec<B256>,
    ) -> Result<Vec<Option<TransactionReceipt>>, RpcError> {
        match self {
            Self::Standard(client) => client.get_transaction_receipts_batch(hashes).await,
            Self::Alchemy(client) => client.get_transaction_receipts_batch(hashes).await,
        }
    }

    pub async fn get_block_receipts(
        &self,
        method_name: &str,
        block_number: BlockNumberOrTag,
    ) -> Result<Vec<Option<TransactionReceipt>>, RpcError> {
        match self {
            Self::Standard(client) => client.get_block_receipts(method_name, block_number).await,
            Self::Alchemy(client) => client.get_block_receipts(method_name, block_number).await,
        }
    }

    pub async fn call_batch(
        &self,
        calls: Vec<(alloy::rpc::types::TransactionRequest, BlockId)>,
    ) -> Result<Vec<Result<Bytes, RpcError>>, RpcError> {
        match self {
            Self::Standard(client) => client.call_batch(calls).await,
            Self::Alchemy(client) => client.call_batch(calls).await,
        }
    }
}

impl std::fmt::Debug for UnifiedRpcClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Standard(client) => f.debug_tuple("UnifiedRpcClient::Standard").field(client).finish(),
            Self::Alchemy(client) => f.debug_tuple("UnifiedRpcClient::Alchemy").field(client).finish(),
        }
    }
}
