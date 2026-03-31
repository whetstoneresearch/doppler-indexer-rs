use crate::transformations::context::TransformationContext;

/// Resolve the migration type string from a migrator contract address.
///
/// Matches the address against all known migrator contracts and returns
/// the corresponding migration type identifier.
pub fn resolve_migration_type(
    ctx: &TransformationContext,
    migrator_address: [u8; 20],
) -> &'static str {
    ctx.match_contract_address(
        migrator_address,
        &[
            "UniswapV4Migrator",
            "UniswapV2Migrator",
            "NimCustomV2Migrator",
            "UniswapV3Migrator",
            "NimCustomV3Migrator",
            "DopplerHookMigrator",
            "RehypeDopplerHookMigrator",
        ],
    )
    .map(|contract_name| match contract_name {
        "UniswapV4Migrator" => "v4",
        "UniswapV2Migrator" | "NimCustomV2Migrator" => "v2",
        "UniswapV3Migrator" | "NimCustomV3Migrator" => "v3",
        "DopplerHookMigrator" => "dhook",
        "RehypeDopplerHookMigrator" => "rehype",
        _ => "unknown",
    })
    .unwrap_or("unknown")
}
