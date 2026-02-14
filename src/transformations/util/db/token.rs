use crate::db::{DbOperation, DbValue};
use crate::transformations::TransformationContext;

pub enum PoolAddressOrPoolId {
    Address([u8; 20]),
    PoolId([u8; 32]),
}

pub fn insert_token(
    block_number: u64,
    block_timestamp: u64,
    tx_hash: &[u8; 32],
    token: &[u8; 20],
    pool: Option<&PoolAddressOrPoolId>,
    name: &str,
    symbol: &str,
    decimals: u8,
    total_supply: &alloy_primitives::Uint<256, 4>,
    token_uri: &str,
    is_derc20: bool,
    is_creator_coin: bool,
    is_content_coin: bool,
    creator_coin_pool: Option<&[u8; 32]>,
    governance: &[u8; 20],
    ctx: &TransformationContext,
) -> DbOperation {
    DbOperation::Insert { 
        table: "tokens".to_string(), 
        columns: vec![
            "chain_id".to_string(),
            "tx_hash".to_string(),
            "block_number".to_string(),
            "created_at".to_string(),
            "creator_address".to_string(),
            "integrator".to_string(),
            "address".to_string(),
            "pool".to_string(),
            "name".to_string()  ,
            "symbol".to_string(),
            "decimals".to_string(),
            "total_supply".to_string(),
            "token_uri".to_string(),
            "is_derc20".to_string(),
            "is_creator_coin".to_string(),
            "is_content_coin".to_string(),
            "creator_coin_pool".to_string(),
            "governance".to_string()
        ], 
        values: vec![
            DbValue::Int64(ctx.chain_id as i64),
            DbValue::Bytes32(*tx_hash),
            DbValue::Uint64(block_number),
            DbValue::Timestamp(block_timestamp as i64),
            DbValue::Address(*token),
            match pool {
                Some(PoolAddressOrPoolId::Address(address)) => DbValue::Address(*address),
                Some(PoolAddressOrPoolId::PoolId(pool_id)) => DbValue::Bytes32(*pool_id),
                None => DbValue::Null,
            },
            DbValue::Text(name.to_string()),
            DbValue::Text(symbol.to_string()),
            DbValue::Int2(decimals),
            DbValue::Numeric(total_supply.to_string()),
            DbValue::Text(token_uri.to_string()),
            DbValue::Bool(is_derc20),
            DbValue::Bool(is_creator_coin),
            DbValue::Bool(is_content_coin),
            match creator_coin_pool {
                Some(creator_coin_pool) => DbValue::Bytes32(*creator_coin_pool),
                None => DbValue::Null,
            },
            DbValue::Address(*governance),
        ]
    }
}