use serde::Serialize;

use crate::db::{DbOperation, DbValue};
use crate::transformations::TransformationContext;
use crate::types::uniswap::v4::PoolKey;

fn serialize_address<S>(address: &[u8; 20], serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&format!("0x{}", hex::encode(address)))
}

#[derive(Serialize)]
pub struct Beneficiary {
    #[serde(serialize_with = "serialize_address")]
    beneficiary: [u8; 20],
    shares: u64
}

pub type BeneficiariesData = Vec<Beneficiary>;

pub enum PoolAddressOrPoolId {
    Address([u8; 20]),
    PoolId([u8; 32]),
}

pub fn insert_pool(
    block_number: u64,
    block_timestamp: u64,
    address: PoolAddressOrPoolId,
    base_token: &[u8; 20],
    quote_token: &[u8; 20],
    is_token_0: bool,
    pool_type: &str,
    integrator: [u8; 20],
    fee: i32,
    min_threshold: u64,
    max_threshold: u64,
    migrator: [u8; 20],
    migrated_at: u64,
    migrated_to: PoolAddressOrPoolId,
    migration_type: &str,
    lock_duration: u32,
    beneficiaries: Option<BeneficiariesData>,
    pool_key: PoolKey,
    starting_time: u64,
    ending_time: u64,
    ctx: TransformationContext
) -> DbOperation {
    DbOperation::Insert {
        table: "pools".to_string(), 
        columns: vec![
            "chain_id".to_string(),
            "block_number".to_string(),
            "created_at".to_string(),
            "address".to_string(),
            "base_token".to_string(),
            "quote_token".to_string(),
            "is_token_0".to_string(),
            "type".to_string(),
            "integrator".to_string(),
            "fee".to_string(),
            "min_threshold".to_string(),
            "max_threshold".to_string(),
            "migrator".to_string(),
            "migrated_at".to_string(),
            "migrated_to".to_string(),
            "lock_duration".to_string(),
            "beneficiaries".to_string(),
            "pool_key".to_string(),
            "starting_time".to_string()
        ], 
        values: vec![
            DbValue::Int64(ctx.chain_id as i64),
            DbValue::Uint64(block_number),
            DbValue::Timestamp(block_timestamp as i64),
            match address {
                PoolAddressOrPoolId::Address(address) => DbValue::Address(address),
                PoolAddressOrPoolId::PoolId(pool_id) => DbValue::Bytes32(pool_id)
            },
            DbValue::Address(*base_token),
            DbValue::Address(*quote_token),
            DbValue::Bool(is_token_0),
            DbValue::VarChar(pool_type.to_string()),
            DbValue::Address(integrator),
            DbValue::Int32(fee),
            DbValue::Int64(min_threshold as i64),
            DbValue::Int64(max_threshold as i64),
            DbValue::Address(migrator),
            DbValue::Timestamp(migrated_at as i64),
            match migrated_to {
                PoolAddressOrPoolId::Address(address) => DbValue::Address(address),
                PoolAddressOrPoolId::PoolId(pool_id) => DbValue::Bytes32(pool_id)
            },
            DbValue::VarChar(migration_type.to_string()),
            DbValue::Int32(lock_duration as i32),
            match beneficiaries {
                Some(beneficiaries_data) => DbValue::jsonb(beneficiaries_data),
                None => DbValue::Null
            },
            DbValue::jsonb(pool_key),
            DbValue::Timestamp(starting_time as i64),
            DbValue::Timestamp(ending_time as i64)
        ]}
}