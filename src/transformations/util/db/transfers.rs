use crate::db::{DbOperation, DbValue};
use crate::transformations::TransformationContext;

#[allow(clippy::too_many_arguments)]
pub fn insert_transfer(
    block_number: u64,
    log_index: u32,
    block_timestamp: u64,
    token: &[u8; 20],
    from: &[u8; 20],
    to: &[u8; 20],
    value: &alloy_primitives::Uint<256, 4>,
    ctx: &TransformationContext,
) -> DbOperation {
    DbOperation::Upsert {
        table: "transfers".to_string(),
        columns: vec![
            "chain_id".to_string(),
            "block_number".to_string(),
            "log_index".to_string(),
            "timestamp".to_string(),
            "token".to_string(),
            "from".to_string(),
            "to".to_string(),
            "value".to_string(),
        ],
        values: vec![
            DbValue::Int64(ctx.chain_id as i64),
            DbValue::Int64(block_number as i64),
            DbValue::Int64(log_index as i64),
            DbValue::Timestamp(block_timestamp as i64),
            DbValue::Address(*token),
            DbValue::Address(*from),
            DbValue::Address(*to),
            DbValue::Numeric(value.to_string()),
        ],
        conflict_columns: vec![
            "chain_id".to_string(),
            "block_number".to_string(),
            "log_index".to_string(),
        ],
        update_columns: vec![],
        update_condition: None,
    }
}
