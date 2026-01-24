use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct RawDataCollectionConfig {
    pub parquet_block_range: Option<u32>,
    pub fields: FieldsConfig,
    pub contract_logs_only: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct FieldsConfig {
    pub block_fields: Option<Vec<BlockField>>,
    pub receipt_fields: Option<Vec<ReceiptField>>,
    pub log_fields: Option<Vec<LogField>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockField {
    Number,
    Timestamp,
    Transactions,
    Uncles,
}

#[derive(Debug, Deserialize)]
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

#[derive(Debug, Deserialize)]
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
