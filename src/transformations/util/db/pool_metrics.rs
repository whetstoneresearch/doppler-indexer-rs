//! Database operation builders for pool metrics tables.
//!
//! Provides functions to construct DbOperations for pool_state, pool_snapshots,
//! and liquidity_deltas tables. These are used by all metrics handlers.

use crate::db::{DbOperation, DbValue};

/// Data for a pool_state upsert.
pub struct PoolStateData {
    pub chain_id: u64,
    pub pool_id: Vec<u8>,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub tick: i32,
    pub sqrt_price_x96: String,
    pub price: String,
    pub active_liquidity: String,
}

/// Data for a pool_snapshots row.
pub struct SnapshotData {
    pub chain_id: u64,
    pub pool_id: Vec<u8>,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub price_open: String,
    pub price_close: String,
    pub price_high: String,
    pub price_low: String,
    pub active_liquidity: String,
    pub volume0: String,
    pub volume1: String,
    pub swap_count: i32,
}

/// Data for a liquidity_deltas row.
pub struct LiquidityDeltaData {
    pub chain_id: u64,
    pub pool_id: Vec<u8>,
    pub block_number: u64,
    pub log_index: u32,
    pub tick_lower: i32,
    pub tick_upper: i32,
    pub liquidity_delta: String,
}

/// Build a pool_state upsert.
///
/// Uses standard DbOperation::Upsert so that live-mode snapshot capture
/// (execute_with_snapshot_capture) can record the previous row state before
/// overwriting. This enables correct rollback on reorg.
///
/// Updates only if incoming block is strictly newer, guarding against
/// out-of-order re-processing.
pub fn upsert_pool_state(data: &PoolStateData) -> DbOperation {
    DbOperation::Upsert {
        table: "pool_state".to_string(),
        columns: vec![
            "chain_id".into(),
            "pool_id".into(),
            "block_number".into(),
            "block_timestamp".into(),
            "tick".into(),
            "sqrt_price_x96".into(),
            "price".into(),
            "active_liquidity".into(),
        ],
        values: vec![
            DbValue::Int64(data.chain_id as i64),
            DbValue::Bytes(data.pool_id.clone()),
            DbValue::Int64(data.block_number as i64),
            DbValue::Int64(data.block_timestamp as i64),
            DbValue::Int32(data.tick),
            DbValue::Numeric(data.sqrt_price_x96.clone()),
            DbValue::Numeric(data.price.clone()),
            DbValue::Numeric(data.active_liquidity.clone()),
        ],
        conflict_columns: vec!["chain_id".into(), "pool_id".into()],
        update_columns: vec![
            "block_number".into(),
            "block_timestamp".into(),
            "tick".into(),
            "sqrt_price_x96".into(),
            "price".into(),
            "active_liquidity".into(),
        ],
        update_condition: Some(
            "EXCLUDED.\"block_number\" > \"pool_state\".\"block_number\"".into(),
        ),
    }
}

/// Build a pool_snapshots upsert (idempotent — updates all columns on conflict).
pub fn insert_pool_snapshot(data: &SnapshotData) -> DbOperation {
    DbOperation::Upsert {
        table: "pool_snapshots".to_string(),
        columns: vec![
            "chain_id".into(),
            "pool_id".into(),
            "block_number".into(),
            "block_timestamp".into(),
            "price_open".into(),
            "price_close".into(),
            "price_high".into(),
            "price_low".into(),
            "active_liquidity".into(),
            "volume0".into(),
            "volume1".into(),
            "swap_count".into(),
        ],
        values: vec![
            DbValue::Int64(data.chain_id as i64),
            DbValue::Bytes(data.pool_id.clone()),
            DbValue::Int64(data.block_number as i64),
            DbValue::Int64(data.block_timestamp as i64),
            DbValue::Numeric(data.price_open.clone()),
            DbValue::Numeric(data.price_close.clone()),
            DbValue::Numeric(data.price_high.clone()),
            DbValue::Numeric(data.price_low.clone()),
            DbValue::Numeric(data.active_liquidity.clone()),
            DbValue::Numeric(data.volume0.clone()),
            DbValue::Numeric(data.volume1.clone()),
            DbValue::Int32(data.swap_count),
        ],
        conflict_columns: vec!["chain_id".into(), "pool_id".into(), "block_number".into()],
        update_columns: vec![
            "block_timestamp".into(),
            "price_open".into(),
            "price_close".into(),
            "price_high".into(),
            "price_low".into(),
            "active_liquidity".into(),
            "volume0".into(),
            "volume1".into(),
            "swap_count".into(),
        ],
        update_condition: None,
    }
}

/// Build a liquidity_deltas upsert (append-only, idempotent on re-runs).
///
/// Uses Upsert with empty update_columns to generate ON CONFLICT DO NOTHING.
/// This prevents duplicate key errors if a handler re-processes the same block range.
pub fn insert_liquidity_delta(data: &LiquidityDeltaData) -> DbOperation {
    DbOperation::Upsert {
        table: "liquidity_deltas".to_string(),
        columns: vec![
            "chain_id".into(),
            "pool_id".into(),
            "block_number".into(),
            "log_index".into(),
            "tick_lower".into(),
            "tick_upper".into(),
            "liquidity_delta".into(),
        ],
        values: vec![
            DbValue::Int64(data.chain_id as i64),
            DbValue::Bytes(data.pool_id.clone()),
            DbValue::Int64(data.block_number as i64),
            DbValue::Int32(i32::try_from(data.log_index).unwrap_or(i32::MAX)),
            DbValue::Int32(data.tick_lower),
            DbValue::Int32(data.tick_upper),
            DbValue::Numeric(data.liquidity_delta.clone()),
        ],
        conflict_columns: vec![
            "chain_id".into(),
            "pool_id".into(),
            "block_number".into(),
            "log_index".into(),
        ],
        update_columns: vec![],
        update_condition: None,
    }
}
