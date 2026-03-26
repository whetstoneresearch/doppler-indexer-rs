use alloy_primitives::U256;

use crate::db::{DbOperation, DbValue};
use crate::transformations::TransformationContext;

/// Domain data for inserting a V4 pool config record into the database.
pub struct PoolConfigData {
    pub pool_id: [u8; 32],
    pub hook_address: [u8; 20],
    pub num_tokens_to_sell: U256,
    pub min_proceeds: U256,
    pub max_proceeds: U256,
    pub starting_time: u64,
    pub ending_time: u64,
    pub starting_tick: i32,
    pub ending_tick: i32,
    pub epoch_length: U256,
    pub gamma: u32,
    pub is_token_0: bool,
    pub num_pd_slugs: U256,
}

pub fn insert_pool_config(data: &PoolConfigData, ctx: &TransformationContext) -> DbOperation {
    DbOperation::Upsert {
        table: "v4_pool_configs".to_string(),
        conflict_columns: vec!["chain_id".to_string(), "hook_address".to_string()],
        update_columns: vec![],
        columns: vec![
            "chain_id".to_string(),
            "pool_id".to_string(),
            "hook_address".to_string(),
            "num_tokens_to_sell".to_string(),
            "min_proceeds".to_string(),
            "max_proceeds".to_string(),
            "starting_time".to_string(),
            "ending_time".to_string(),
            "starting_tick".to_string(),
            "ending_tick".to_string(),
            "epoch_length".to_string(),
            "gamma".to_string(),
            "is_token_0".to_string(),
            "num_pd_slugs".to_string(),
        ],
        values: vec![
            DbValue::Int64(ctx.chain_id as i64),
            DbValue::Bytes32(data.pool_id),
            DbValue::Address(data.hook_address),
            DbValue::Numeric(data.num_tokens_to_sell.to_string()),
            DbValue::Numeric(data.min_proceeds.to_string()),
            DbValue::Numeric(data.max_proceeds.to_string()),
            DbValue::Timestamp(data.starting_time as i64),
            DbValue::Timestamp(data.ending_time as i64),
            DbValue::Int32(data.starting_tick),
            DbValue::Int32(data.ending_tick),
            DbValue::Numeric(data.epoch_length.to_string()),
            DbValue::Int32(data.gamma as i32),
            DbValue::Bool(data.is_token_0),
            DbValue::Numeric(data.num_pd_slugs.to_string()),
        ],
    }
}
