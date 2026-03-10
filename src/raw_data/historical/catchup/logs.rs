use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use crate::raw_data::historical::logs::{
    build_configured_addresses, build_log_schema, scan_existing_parquet_files, LogCollectionError,
    LogsCatchupState,
};
use crate::storage::{S3Manifest, StorageManager};
use crate::types::config::chain::ChainConfig;
use crate::types::config::raw_data::RawDataCollectionConfig;

pub async fn collect_logs(
    chain: &ChainConfig,
    raw_data_config: &RawDataCollectionConfig,
    s3_manifest: Option<S3Manifest>,
    storage_manager: Option<Arc<StorageManager>>,
) -> Result<LogsCatchupState, LogCollectionError> {
    let output_dir = PathBuf::from(format!("data/{}/historical/raw/logs", chain.name));
    std::fs::create_dir_all(&output_dir)?;

    let range_size = raw_data_config.parquet_block_range.unwrap_or(1000) as u64;
    let log_fields = &raw_data_config.fields.log_fields;
    let schema = build_log_schema(log_fields);
    let contract_logs_only = raw_data_config.contract_logs_only.unwrap_or(false);

    let configured_addresses: HashSet<[u8; 20]> = if contract_logs_only {
        build_configured_addresses(&chain.contracts)
    } else {
        HashSet::new()
    };

    let existing_files = scan_existing_parquet_files(&output_dir);

    Ok(LogsCatchupState {
        output_dir,
        range_size,
        schema,
        configured_addresses,
        existing_files,
        contract_logs_only,
        needs_factory_wait: false, // set by caller based on factory_rx presence
        log_fields: log_fields.clone(),
        s3_manifest,
        storage_manager,
        chain_name: chain.name.clone(),
    })
}
