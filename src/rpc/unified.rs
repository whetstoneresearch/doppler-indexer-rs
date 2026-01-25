use alloy::primitives::BlockNumber;
use alloy::rpc::types::{Block, BlockNumberOrTag};

use crate::rpc::alchemy::AlchemyClient;
use crate::rpc::rpc::{RpcClient, RpcError};

/// Unified RPC client that automatically selects between standard RPC and Alchemy
/// based on the URL. Uses Alchemy's compute unit rate limiting when the URL
/// contains "alchemy".
pub enum UnifiedRpcClient {
    Standard(RpcClient),
    Alchemy(AlchemyClient),
}

impl UnifiedRpcClient {
    /// Creates a new UnifiedRpcClient from a URL string.
    /// Automatically detects Alchemy URLs and uses compute unit rate limiting.
    pub fn from_url(url: &str) -> Result<Self, RpcError> {
        if url.contains("alchemy") {
            // Default 330 CU/s for Alchemy free tier
            Ok(Self::Alchemy(AlchemyClient::from_url(url, 330)?))
        } else {
            Ok(Self::Standard(RpcClient::from_url(url)?))
        }
    }

    /// Creates a new UnifiedRpcClient from a URL with custom Alchemy compute units per second.
    /// Falls back to standard RPC if URL doesn't contain "alchemy".
    pub fn from_url_with_alchemy_cu(url: &str, compute_units_per_second: u32) -> Result<Self, RpcError> {
        if url.contains("alchemy") {
            Ok(Self::Alchemy(AlchemyClient::from_url(url, compute_units_per_second)?))
        } else {
            Ok(Self::Standard(RpcClient::from_url(url)?))
        }
    }

    /// Gets the current block number.
    pub async fn get_block_number(&self) -> Result<BlockNumber, RpcError> {
        match self {
            Self::Standard(client) => client.get_block_number().await,
            Self::Alchemy(client) => client.get_block_number().await,
        }
    }

    /// Gets multiple blocks in a batch.
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
}

impl std::fmt::Debug for UnifiedRpcClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Standard(client) => f.debug_tuple("UnifiedRpcClient::Standard").field(client).finish(),
            Self::Alchemy(client) => f.debug_tuple("UnifiedRpcClient::Alchemy").field(client).finish(),
        }
    }
}
