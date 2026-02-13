use crate::db::{DbOperation, DbValue};
use crate::transformations::TransformationContext;

pub fn insert_transfer(
    block_number: u64,
    block_timestamp: u64,
    token: &[u8; 20],
    from: &[u8; 20],
    to: &[u8; 20],
    value: &alloy_primitives::Uint<256, 4>,
    ctx: &TransformationContext,
) -> DbOperation {
    DbOperation::Insert {
        table: "transfers".to_string(),
        columns: vec![
            "chain_id".to_string(),
            "block_number".to_string(),
            "timestamp".to_string(),
            "token".to_string(),
            "from".to_string(),
            "to".to_string(),
            "value".to_string(),
        ],
        values: vec![
            DbValue::Int64(ctx.chain_id as i64),
            DbValue::Int64(block_number as i64),
            DbValue::Timestamp(block_timestamp as i64),
            DbValue::Address(*token),
            DbValue::Address(*from),
            DbValue::Address(*to),
            DbValue::Numeric(value.to_string()),
        ],
    }
}