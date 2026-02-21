use alloy::primitives::B256;
use tokio::sync::mpsc::Sender;

use crate::raw_data::historical::blocks::BlockCollectionError;
use crate::rpc::UnifiedRpcClient;
use crate::types::config::chain::ChainConfig;
use crate::types::config::raw_data::RawDataCollectionConfig;

pub async fn collect_blocks(
    _chain: &ChainConfig,
    _client: &UnifiedRpcClient,
    _raw_data_config: &RawDataCollectionConfig,
    _from_block: u64,
    _tx_sender: Option<Sender<(u64, u64, Vec<B256>)>>,
    _eth_call_sender: Option<Sender<(u64, u64)>>,
) -> Result<(), BlockCollectionError> {
    Ok(())
}
