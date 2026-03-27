use alloy_primitives::U256;

use crate::db::{DbOperation, DbValue};
use crate::transformations::TransformationContext;

/// Domain data for inserting a DopplerHook pool config record into the database.
pub struct DhookPoolConfigData {
    pub pool_id: [u8; 32],
    pub hook_address: [u8; 20],
    pub numeraire: [u8; 20],
    pub total_tokens_on_bonding_curve: U256,
    pub doppler_hook: [u8; 20],
    pub status: u8,
    pub far_tick: i32,
    pub is_token_0: bool,
}

pub fn insert_dhook_pool_config(data: &DhookPoolConfigData, ctx: &TransformationContext) -> DbOperation {
    DbOperation::Upsert {
        table: "dhook_pool_configs".to_string(),
        conflict_columns: vec!["chain_id".to_string(), "hook_address".to_string()],
        update_columns: vec![],
        columns: vec![
            "chain_id".to_string(),
            "pool_id".to_string(),
            "hook_address".to_string(),
            "numeraire".to_string(),
            "total_tokens_on_bonding_curve".to_string(),
            "doppler_hook".to_string(),
            "status".to_string(),
            "far_tick".to_string(),
            "is_token_0".to_string(),
        ],
        values: vec![
            DbValue::Int64(ctx.chain_id as i64),
            DbValue::Bytes32(data.pool_id),
            DbValue::Address(data.hook_address),
            DbValue::Address(data.numeraire),
            DbValue::Numeric(data.total_tokens_on_bonding_curve.to_string()),
            DbValue::Address(data.doppler_hook),
            DbValue::Int2(data.status as i16),
            DbValue::Int32(data.far_tick),
            DbValue::Bool(data.is_token_0),
        ],
    }
}
