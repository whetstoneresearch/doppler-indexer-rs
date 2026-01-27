use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct RawDataCollectionConfig {
    pub parquet_block_range: Option<u32>,
    pub rpc_batch_size: Option<u32>,
    pub fields: FieldsConfig,
    pub contract_logs_only: Option<bool>,
    /// Capacity for main channels (blocks, logs, eth_calls). Default: 1000
    pub channel_capacity: Option<usize>,
    /// Capacity for factory-related channels. Default: 1000
    pub factory_channel_capacity: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FieldsConfig {
    pub block_fields: Option<Vec<BlockField>>,
    pub receipt_fields: Option<Vec<ReceiptField>>,
    pub log_fields: Option<Vec<LogField>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockField {
    Number,
    Timestamp,
    Transactions,
    Uncles,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReceiptField {
    BlockNumber,
    #[serde(alias = "timestamp")]
    BlockTimestamp,
    TransactionHash,
    From,
    To,
    Logs,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogField {
    BlockNumber,
    #[serde(alias = "timestamp")]
    BlockTimestamp,
    TransactionHash,
    LogIndex,
    Address,
    Topics,
    Data,
}
