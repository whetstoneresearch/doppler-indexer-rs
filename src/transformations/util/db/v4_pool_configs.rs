use alloy_primitives::U256;

use crate::db::{DbOperation, DbValue};
use crate::transformations::TransformationContext;

pub fn insert_pool_config(
    block_number: u64,
    block_timestamp: u64,
    pool_id: [u8; 32],
    num_tokens_to_sell: U256,
    min_proceeds: U256,
    max_proceeds: U256,
    starting_time: u64,
    ending_time: u64,
    starting_tick: i32,
    ending_tick: i32,
    epoch_length: U256,
    gamma: u32,
    is_token_0: bool,
    num_pds_slugs: U256,
    ctx: &TransformationContext
) -> DbOperation {
    DbOperation::Insert {
        table: "v4_pool_configs".to_string(), 
        columns: vec![
            "chain_id".to_string(),
            "block_number".to_string(),
            "created_at".to_string(),
            "pool_id".to_string(),
            "min_proceeds".to_string(),
            "max_proceeds".to_string(),
            "starting_time".to_string(),
            "ending_time".to_string(),
            "starting_tick".to_string(),
            "ending_tick".to_string(),
            "epoch_length".to_string(),
            "gamma".to_string(),
            "is_token_0".to_string(),
            "num_pds_slugs".to_string()
        ], 
        values: vec![
            DbValue::Int64(ctx.chain_id as i64),
            DbValue::Uint64(block_number),
            DbValue::Timestamp(block_timestamp as i64),
            DbValue::Bytes32(pool_id),
            DbValue::Numeric(num_tokens_to_sell.to_string()),
            DbValue::Numeric(min_proceeds.to_string()),
            DbValue::Numeric(max_proceeds.to_string()),
            DbValue::Timestamp(starting_time as i64),
            DbValue::Timestamp(ending_time as i64),
            DbValue::Int32(starting_tick),
            DbValue::Int32(ending_tick),
            DbValue::Numeric(epoch_length.to_string()),
            DbValue::Int32(gamma as i32),
            DbValue::Bool(is_token_0),
            DbValue::Numeric(num_pds_slugs.to_string())
        ]}
}