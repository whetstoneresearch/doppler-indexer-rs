use alloy_primitives::{Address, B256};

use crate::transformations::util::sanitize::is_precompile_address;

use crate::types::shared::metadata::{TokenMetadata, AssetTokenMetadata};
use crate::types::uniswap::v4::PoolAddressOrPoolId;
use crate::transformations::{DecodedEvent, TransformationContext, TransformationError};

pub fn get_metadata(asset: &[u8; 20], numeraire: &[u8; 20], event: &DecodedEvent, ctx: &TransformationContext) -> Result<(AssetTokenMetadata, TokenMetadata), TransformationError> {
    if is_precompile_address(asset.into()) == true {
        return Err(TransformationError::IncludesPrecompileError("asset address is a precompile".to_string()))
    } else if is_precompile_address(numeraire.into()) == true {
        return Err(TransformationError::IncludesPrecompileError("numeraire address is a precompile".to_string()))
    }

    let asset_metadata = ctx.calls_for_address(*asset)
        .filter(|call| call.function_name == "once")
        .next()
        .ok_or_else(|| {
            let available_calls: Vec<_> = ctx.calls_for_address(*asset)
                .map(|c| format!("{}:{}", c.source_name, c.function_name))
                .collect();
            TransformationError::MissingData(format!(
                "No 'once' call found for asset {} at block {} tx {}. Available calls: {:?}",
                Address::from(asset), event.block_number, B256::from(event.transaction_hash), available_calls
            ))
        })?;

    let asset_metadata = {
        let call = asset_metadata;
        let field_err = |field: &str, expected: &str| {
            TransformationError::TypeConversion(format!(
                "asset {} field '{}': expected {} but got {:?} at block {} tx {}",
                Address::from(asset), field, expected, call.result.get(field),
                event.block_number, B256::from(event.transaction_hash)
            ))
        };
        let missing_err = |field: &str| {
            TransformationError::MissingData(format!(
                "asset {} missing field '{}' at block {} tx {}. Available fields: {:?}",
                Address::from(asset), field, event.block_number, B256::from(event.transaction_hash),
                call.result.keys().collect::<Vec<_>>()
            ))
        };

        AssetTokenMetadata {
            name: call.result.get("name")
                .ok_or_else(|| missing_err("name"))?
                .as_string()
                .ok_or_else(|| field_err("name", "string"))?
                .to_string(),
            symbol: call.result.get("symbol")
                .ok_or_else(|| missing_err("symbol"))?
                .as_string()
                .ok_or_else(|| field_err("symbol", "string"))?
                .to_string(),
            decimals: 18,
            token_uri: call.result.get("tokenURI")
                .ok_or_else(|| missing_err("tokenURI"))?
                .as_string()
                .ok_or_else(|| field_err("tokenURI", "string"))?
                .to_string(),
            total_supply: call.result.get("getAssetData.totalSupply")
                .ok_or_else(|| missing_err("getAssetData.totalSupply"))?
                .as_uint256()
                .ok_or_else(|| field_err("getAssetData.totalSupply", "uint256"))?,
            governance: call.result.get("getAssetData.governance")
                .ok_or_else(|| missing_err("getAssetData.governance"))?
                .as_address()
                .ok_or_else(|| field_err("getAssetData.governance", "address"))?
                .into(),
            integrator: call.result.get("getAssetData.integrator")
                .ok_or_else(|| missing_err("getAssetData.integrator"))?
                .as_address()
                .ok_or_else(|| field_err("getAssetData.integrator", "address"))?
                .into(),
            initializer: call.result.get("getAssetData.poolInitializer")
                .ok_or_else(|| missing_err("getAssetData.poolInitializer"))?
                .as_address()
                .ok_or_else(|| field_err("getAssetData.poolInitializer", "address"))?
                .into(),
            migrator: call.result.get("getAssetData.liquidityMigrator")
                .ok_or_else(|| missing_err("getAssetData.liquidityMigrator"))?
                .as_address()
                .ok_or_else(|| field_err("getAssetData.liquidityMigrator", "address"))?
                .into(),
            migration_pool: {
                let val = call.result.get("getAssetData.migrationPool")
                    .ok_or_else(|| missing_err("getAssetData.migrationPool"))?;
                if let Some(addr) = val.as_address() {
                    PoolAddressOrPoolId::Address(addr.into())
                } else {
                    PoolAddressOrPoolId::PoolId(
                        val.as_bytes32()
                            .ok_or_else(|| field_err("getAssetData.migrationPool", "address or bytes32"))?
                    )
                }
            }
        }
    };

    let numeraire_metadata = if Address::from(numeraire).is_zero() {
        // Native ETH represented as zero address
        TokenMetadata {
            name: "Native Ether".to_string(),
            symbol: "ETH".to_string(),
            decimals: 18,
        }
    } else {
        let call = ctx.calls_for_address(*numeraire)
            .filter(|call| call.function_name == "once")
            .next()
            .ok_or_else(|| {
                let available_calls: Vec<_> = ctx.calls_for_address(*numeraire)
                    .map(|c| format!("{}:{}", c.source_name, c.function_name))
                    .collect();
                TransformationError::MissingData(format!(
                    "No 'once' call found for numeraire {} at block {} tx {}. Available calls: {:?}",
                    Address::from(numeraire), event.block_number, B256::from(event.transaction_hash), available_calls
                ))
            })?;

        let field_err = |field: &str, expected: &str| {
            TransformationError::TypeConversion(format!(
                "numeraire {} field '{}': expected {} but got {:?} at block {} tx {}",
                Address::from(numeraire), field, expected, call.result.get(field),
                event.block_number, B256::from(event.transaction_hash)
            ))
        };
        let missing_err = |field: &str| {
            TransformationError::MissingData(format!(
                "numeraire {} missing field '{}' at block {} tx {}. Available fields: {:?}",
                Address::from(numeraire), field, event.block_number, B256::from(event.transaction_hash),
                call.result.keys().collect::<Vec<_>>()
            ))
        };

        TokenMetadata {
            name: call.result.get("name")
                .ok_or_else(|| missing_err("name"))?
                .as_string()
                .ok_or_else(|| field_err("name", "string"))?
                .to_string(),
            symbol: call.result.get("symbol")
                .ok_or_else(|| missing_err("symbol"))?
                .as_string()
                .ok_or_else(|| field_err("symbol", "string"))?
                .to_string(),
            decimals: call.result.get("decimals")
                .ok_or_else(|| missing_err("decimals"))?
                .as_u8()
                .ok_or_else(|| field_err("decimals", "u8"))?,
        }
    };

    return Ok((asset_metadata, numeraire_metadata))
}