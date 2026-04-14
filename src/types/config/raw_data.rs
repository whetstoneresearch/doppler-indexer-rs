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
    /// Number of concurrent receipt fetch tasks.
    /// In block-receipt mode each task fetches one block; in fallback mode each
    /// task fetches one tx-receipt micro-batch.
    /// Higher values improve throughput but use more memory and compute units.
    /// Default: 10
    pub block_receipt_concurrency: Option<usize>,
    /// Number of concurrent decoding tasks for log and eth_call decoding.
    /// Higher values improve throughput during catchup but use more memory.
    /// Default: 4
    pub decoding_concurrency: Option<usize>,
    /// Number of concurrent tasks for factory collection catchup from existing log files.
    /// Higher values improve throughput during catchup but use more memory.
    /// Default: 4
    pub factory_concurrency: Option<usize>,
    /// Number of concurrent log ranges to process for event-triggered eth_call catchup.
    /// Higher values improve throughput during catchup but use more RPC bandwidth.
    /// Default: 4
    pub event_call_concurrency: Option<usize>,
    /// Maximum number of event-call log-range tasks alive in the JoinSet at once.
    /// Lower values reduce peak memory; higher values improve pipeline throughput.
    /// Default: event_call_concurrency * 3
    pub event_call_window_size: Option<usize>,
    /// Maximum number of event triggers to process per RPC batch within a single range.
    /// Lower values reduce peak memory; set to 0 to disable chunking (old behavior).
    /// Default: 50000
    pub event_call_trigger_batch_size: Option<usize>,
    /// Enable WebSocket live mode after catchup completes.
    /// Requires ws_url_env_var to be set in chain config.
    pub live_mode: Option<bool>,
    /// Number of blocks to track for reorg detection.
    /// Default: 128
    pub reorg_depth: Option<u64>,
    /// Interval in seconds between compaction checks.
    /// Default: 10
    pub compaction_interval_secs: Option<u64>,
    /// Grace period in seconds before retrying stuck transformations.
    /// Default: 300
    pub transform_retry_grace_period_secs: Option<u64>,
    /// Maximum number of concurrent receipt batch ranges held in memory.
    /// Lower values reduce peak memory at the cost of throughput.
    /// Default: 5
    pub max_receipt_ranges: Option<usize>,
    /// Maximum number of log ranges buffered in the log and factory collectors
    /// before backpressure stops draining from the channel.
    /// Lower values reduce peak memory; higher values improve pipeline throughput.
    /// Default: 10
    pub max_pending_log_ranges: Option<usize>,
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
