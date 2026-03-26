use alloy_primitives::U256;
use serde::Serialize;

use crate::db::{DbOperation, DbValue};
use crate::transformations::TransformationContext;

use crate::types::uniswap::v4::{PoolAddressOrPoolId, PoolKey};

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
    shares: u64,
}

impl Beneficiary {
    pub fn new(beneficiary: [u8; 20], shares: u64) -> Self {
        Self {
            beneficiary,
            shares,
        }
    }
}

pub type BeneficiariesData = Vec<Beneficiary>;

/// Domain data for inserting a pool record into the database.
pub struct PoolData {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub address: PoolAddressOrPoolId,
    pub base_token: [u8; 20],
    pub quote_token: [u8; 20],
    pub is_token_0: bool,
    pub pool_type: String,
    pub integrator: [u8; 20],
    pub initializer: [u8; 20],
    pub fee: u32,
    pub min_threshold: U256,
    pub max_threshold: U256,
    pub migrator: [u8; 20],
    pub migrated_at: Option<u64>,
    pub migration_pool: PoolAddressOrPoolId,
    pub migration_type: String,
    pub lock_duration: Option<u32>,
    pub beneficiaries: Option<BeneficiariesData>,
    pub pool_key: PoolKey,
    pub starting_time: u64,
    pub ending_time: u64,
}

pub fn insert_pool(data: &PoolData, ctx: &TransformationContext) -> DbOperation {
    DbOperation::Upsert {
        table: "pools".to_string(),
        conflict_columns: vec!["chain_id".to_string(), "address".to_string()],
        update_columns: vec![],
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
            "initializer".to_string(),
            "fee".to_string(),
            "min_threshold".to_string(),
            "max_threshold".to_string(),
            "migrator".to_string(),
            "migrated_at".to_string(),
            "migration_pool".to_string(),
            "migration_type".to_string(),
            "lock_duration".to_string(),
            "beneficiaries".to_string(),
            "pool_key".to_string(),
            "starting_time".to_string(),
            "ending_time".to_string(),
        ],
        values: vec![
            DbValue::Int64(ctx.chain_id as i64),
            DbValue::Uint64(data.block_number),
            DbValue::Timestamp(data.block_timestamp as i64),
            match &data.address {
                PoolAddressOrPoolId::Address(address) => DbValue::Address(*address),
                PoolAddressOrPoolId::PoolId(pool_id) => DbValue::Bytes32(*pool_id),
            },
            DbValue::Address(data.base_token),
            DbValue::Address(data.quote_token),
            DbValue::Bool(data.is_token_0),
            DbValue::VarChar(data.pool_type.clone()),
            DbValue::Address(data.integrator),
            DbValue::Address(data.initializer),
            DbValue::Int32(data.fee as i32),
            DbValue::Numeric(data.min_threshold.to_string()),
            DbValue::Numeric(data.max_threshold.to_string()),
            DbValue::Address(data.migrator),
            match data.migrated_at {
                Some(timestamp) => DbValue::Timestamp(timestamp as i64),
                None => DbValue::Null,
            },
            match &data.migration_pool {
                PoolAddressOrPoolId::Address(address) => DbValue::Address(*address),
                PoolAddressOrPoolId::PoolId(pool_id) => DbValue::Bytes32(*pool_id),
            },
            DbValue::VarChar(data.migration_type.clone()),
            match data.lock_duration {
                Some(duration) => DbValue::Int32(duration as i32),
                None => DbValue::Null,
            },
            match &data.beneficiaries {
                Some(beneficiaries_data) => DbValue::jsonb(beneficiaries_data),
                None => DbValue::Null,
            },
            DbValue::jsonb(&data.pool_key),
            DbValue::Timestamp(data.starting_time as i64),
            DbValue::Timestamp(data.ending_time as i64),
        ],
    }
}
