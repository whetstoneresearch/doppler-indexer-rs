use crate::db::{DbOperation, DbValue};
use crate::transformations::TransformationContext;

pub fn upsert_user(address: &[u8; 20], block_timestamp: &u64, ctx: &TransformationContext) -> DbOperation {
    DbOperation::Upsert {
        table: "users".to_string(),
        columns: vec![
            "chain_id".to_string(),
            "address".to_string(),
            "first_seen".to_string(),
            "last_seen".to_string(),
        ],
        values: vec![
            DbValue::Int64(ctx.chain_id as i64),
            DbValue::Address(*address),
            DbValue::Timestamp(*block_timestamp as i64),
            DbValue::Timestamp(*block_timestamp as i64),
        ],
        conflict_columns: vec![
            "chain_id".to_string(),
            "address".to_string(),
        ],
        update_columns: vec![
            "last_seen".to_string(),
        ],
    }
}