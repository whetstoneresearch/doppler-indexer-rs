use crate::db::{DbOperation, DbValue};
use crate::transformations::TransformationContext;
use crate::types::uniswap::v4::PoolAddressOrPoolId;

/// Domain data for inserting a token record into the database.
///
/// Uses lifetime references to avoid unnecessary cloning at call sites,
/// since most data comes from decoded events and metadata that outlive
/// the insert call.
pub struct TokenData<'a> {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub tx_id: &'a [u8],
    pub creator_address: Option<&'a [u8]>,
    pub integrator: Option<&'a [u8]>,
    pub token_address: &'a [u8],
    pub pool: Option<&'a PoolAddressOrPoolId>,
    pub name: &'a str,
    pub symbol: &'a str,
    pub decimals: u8,
    pub total_supply: Option<&'a alloy_primitives::Uint<256, 4>>,
    pub token_uri: Option<&'a str>,
    pub is_derc20: bool,
    pub is_creator_coin: bool,
    pub is_content_coin: bool,
    pub creator_coin_pool: Option<&'a [u8]>,
    pub governance: Option<&'a [u8]>,
}

pub fn insert_token(data: &TokenData<'_>, ctx: &TransformationContext) -> DbOperation {
    DbOperation::Upsert {
        table: "tokens".to_string(),
        conflict_columns: vec!["chain_id".to_string(), "address".to_string()],
        update_columns: vec![],
        update_condition: None,
        columns: vec![
            "chain_id".to_string(),
            "tx_id".to_string(),
            "block_height".to_string(),
            "created_at".to_string(),
            "creator_address".to_string(),
            "integrator".to_string(),
            "address".to_string(),
            "pool".to_string(),
            "name".to_string(),
            "symbol".to_string(),
            "decimals".to_string(),
            "total_supply".to_string(),
            "token_uri".to_string(),
            "is_derc20".to_string(),
            "is_creator_coin".to_string(),
            "is_content_coin".to_string(),
            "creator_coin_pool".to_string(),
            "governance".to_string(),
        ],
        values: vec![
            DbValue::Int64(ctx.chain_id as i64),
            DbValue::Bytes(data.tx_id.to_vec()),
            DbValue::Uint64(data.block_number),
            DbValue::Timestamp(data.block_timestamp as i64),
            match data.creator_address {
                Some(creator) => DbValue::Bytes(creator.to_vec()),
                None => DbValue::Null,
            },
            match data.integrator {
                Some(int_addr) => DbValue::Bytes(int_addr.to_vec()),
                None => DbValue::Null,
            },
            DbValue::Bytes(data.token_address.to_vec()),
            match data.pool {
                Some(PoolAddressOrPoolId::Address(address)) => DbValue::Bytes(address.to_vec()),
                Some(PoolAddressOrPoolId::PoolId(pool_id)) => DbValue::Bytes(pool_id.to_vec()),
                None => DbValue::Null,
            },
            DbValue::Text(data.name.to_string()),
            DbValue::Text(data.symbol.to_string()),
            DbValue::Int2(data.decimals as i16),
            match data.total_supply {
                Some(supply) => DbValue::Numeric(supply.to_string()),
                None => DbValue::Null,
            },
            match data.token_uri {
                Some(uri) => DbValue::Text(uri.to_string()),
                None => DbValue::Null,
            },
            DbValue::Bool(data.is_derc20),
            DbValue::Bool(data.is_creator_coin),
            DbValue::Bool(data.is_content_coin),
            match data.creator_coin_pool {
                Some(creator_coin_pool) => DbValue::Bytes(creator_coin_pool.to_vec()),
                None => DbValue::Null,
            },
            match data.governance {
                Some(gov_addr) => DbValue::Bytes(gov_addr.to_vec()),
                None => DbValue::Null,
            },
        ],
    }
}
