mod raw_data;
mod rpc;
mod types;

use std::path::Path;
use std::env;

use tokio::sync::mpsc;
use tracing_subscriber::EnvFilter;

use raw_data::historical::blocks::collect_blocks;
use raw_data::historical::receipts::collect_receipts;
use raw_data::historical::logs::collect_logs;
use rpc::UnifiedRpcClient;
use types::config::indexer::IndexerConfig;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info"))
        )
        .init();

    let config = IndexerConfig::load(Path::new("config/config.json"))?;

    tracing::info!("Loaded config with {} chain(s)", config.chains.len());

    for chain in &config.chains {
        tracing::info!("Processing chain: {}", chain.name);

        let rpc_url = env::var(&chain.rpc_url_env_var)
            .map_err(|_| anyhow::anyhow!(
                "Environment variable {} not set for chain {}",
                chain.rpc_url_env_var,
                chain.name
            ))?;

        let client = UnifiedRpcClient::from_url(&rpc_url)?;
        tracing::info!("Connected to RPC for chain {}", chain.name);

        let (block_tx, block_rx) = mpsc::channel(1000);
        let (log_tx, log_rx) = mpsc::channel(1000);

        let raw_data_config = config.raw_data_collection.clone();
        let raw_data_config2 = config.raw_data_collection.clone();
        let raw_data_config3 = config.raw_data_collection.clone();
        let chain_clone = chain.clone();
        let chain_clone2 = chain.clone();
        let chain_clone3 = chain.clone();

        let blocks_handle = tokio::spawn(async move {
            collect_blocks(&chain_clone, &client, &raw_data_config, Some(block_tx)).await
        });

        let receipts_handle = tokio::spawn(async move {
            let rpc_url = env::var(&chain_clone2.rpc_url_env_var).unwrap();
            let client = UnifiedRpcClient::from_url(&rpc_url).unwrap();
            collect_receipts(&chain_clone2, &client, &raw_data_config2, block_rx, Some(log_tx)).await
        });

        let logs_handle = tokio::spawn(async move {
            collect_logs(&chain_clone3, &raw_data_config3, log_rx).await
        });

        let (blocks_result, receipts_result, logs_result) =
            tokio::try_join!(blocks_handle, receipts_handle, logs_handle)?;

        blocks_result?;
        receipts_result?;
        logs_result?;

        tracing::info!("Completed collection for chain {}", chain.name);
    }

    tracing::info!("All chains processed successfully");
    Ok(())
}
