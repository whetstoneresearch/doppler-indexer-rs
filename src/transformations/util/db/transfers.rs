use crate::db::{DbOperation, DbValue};
use crate::transformations::TransformationContext;

#[allow(clippy::too_many_arguments)]
pub fn insert_transfer(
    block_number: u64,
    log_position: i64,
    block_timestamp: u64,
    token: &[u8],
    from: &[u8],
    to: &[u8],
    value: &alloy_primitives::Uint<256, 4>,
    ctx: &TransformationContext,
) -> DbOperation {
    DbOperation::Upsert {
        table: "transfers".to_string(),
        columns: vec![
            "chain_id".to_string(),
            "block_height".to_string(),
            "log_position".to_string(),
            "timestamp".to_string(),
            "token".to_string(),
            "from".to_string(),
            "to".to_string(),
            "value".to_string(),
        ],
        values: vec![
            DbValue::Int64(ctx.chain_id as i64),
            DbValue::Int64(block_number as i64),
            DbValue::Int64(log_position),
            DbValue::Timestamp(block_timestamp as i64),
            DbValue::Bytes(token.to_vec()),
            DbValue::Bytes(from.to_vec()),
            DbValue::Bytes(to.to_vec()),
            DbValue::Numeric(value.to_string()),
        ],
        conflict_columns: vec![
            "chain_id".to_string(),
            "block_height".to_string(),
            "log_position".to_string(),
        ],
        update_columns: vec![],
        update_condition: None,
    }
}
