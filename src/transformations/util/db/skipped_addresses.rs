use crate::db::{DbOperation, DbValue};
use crate::transformations::TransformationContext;

pub struct SkippedAddressData<'a> {
    pub block_number: u64,
    pub tx_id: &'a [u8],
    pub asset_address: &'a [u8],
    pub numeraire_address: &'a [u8],
    pub reason: &'a str,
}

pub fn insert_skipped_address(
    data: &SkippedAddressData<'_>,
    ctx: &TransformationContext,
) -> DbOperation {
    DbOperation::Upsert {
        table: "_skipped_addresses".to_string(),
        columns: vec![
            "chain_id".to_string(),
            "block_height".to_string(),
            "tx_id".to_string(),
            "asset_address".to_string(),
            "numeraire_address".to_string(),
            "reason".to_string(),
        ],
        values: vec![
            DbValue::Int64(ctx.chain_id as i64),
            DbValue::Int64(data.block_number as i64),
            DbValue::Bytes(data.tx_id.to_vec()),
            DbValue::Bytes(data.asset_address.to_vec()),
            DbValue::Bytes(data.numeraire_address.to_vec()),
            DbValue::Text(data.reason.to_string()),
        ],
        conflict_columns: vec![
            "chain_id".to_string(),
            "tx_id".to_string(),
            "asset_address".to_string(),
        ],
        update_columns: vec![],
        update_condition: None,
    }
}
