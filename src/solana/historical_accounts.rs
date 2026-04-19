use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use anyhow::{anyhow, Context};

use crate::solana::decoding::decoded_parquet::{
    build_decoded_account_state_schema, read_decoded_events_from_parquet,
    write_decoded_account_states_to_parquet,
};
use crate::storage::paths::{
    decoded_solana_accounts_dir, decoded_solana_events_dir, scan_parquet_ranges,
};
use crate::transformations::context::{DecodedAccountState, FieldExtractor};
use crate::types::chain::{ChainAddress, TxId};
use crate::types::config::chain::ChainConfig;
use crate::types::decoded::DecodedValue;
use crate::types::shared::repair::RepairScope;

const CPMM_SOURCE: &str = "DopplerCPMM";
const CPMM_POOL_TYPE: &str = "Pool";
const CPMM_EVENTS: [&str; 4] = ["PoolInitialized", "AddLiquidity", "RemoveLiquidity", "Swap"];

#[derive(Clone, Debug, Default)]
struct SyntheticCpmmPoolState {
    token0_mint: Option<[u8; 32]>,
    token1_mint: Option<[u8; 32]>,
    vault0: Option<[u8; 32]>,
    vault1: Option<[u8; 32]>,
    reserve0: u64,
    reserve1: u64,
    total_shares: u128,
}

impl SyntheticCpmmPoolState {
    fn to_decoded_account_state(
        &self,
        owner_program: [u8; 32],
        pool: [u8; 32],
        slot: u64,
        block_timestamp: u64,
    ) -> DecodedAccountState {
        let mut fields = HashMap::new();
        if let Some(token0_mint) = self.token0_mint {
            fields.insert(
                Arc::<str>::from("token0_mint"),
                DecodedValue::ChainAddress(ChainAddress::Solana(token0_mint)),
            );
        }
        if let Some(token1_mint) = self.token1_mint {
            fields.insert(
                Arc::<str>::from("token1_mint"),
                DecodedValue::ChainAddress(ChainAddress::Solana(token1_mint)),
            );
        }
        if let Some(vault0) = self.vault0 {
            fields.insert(
                Arc::<str>::from("vault0"),
                DecodedValue::ChainAddress(ChainAddress::Solana(vault0)),
            );
        }
        if let Some(vault1) = self.vault1 {
            fields.insert(
                Arc::<str>::from("vault1"),
                DecodedValue::ChainAddress(ChainAddress::Solana(vault1)),
            );
        }
        fields.insert(
            Arc::<str>::from("reserve0"),
            DecodedValue::Uint64(self.reserve0),
        );
        fields.insert(
            Arc::<str>::from("reserve1"),
            DecodedValue::Uint64(self.reserve1),
        );
        fields.insert(
            Arc::<str>::from("total_shares"),
            DecodedValue::Uint128(self.total_shares),
        );

        DecodedAccountState {
            block_number: slot,
            block_timestamp,
            account_address: ChainAddress::Solana(pool),
            owner_program: ChainAddress::Solana(owner_program),
            source_name: CPMM_SOURCE.to_string(),
            account_type: CPMM_POOL_TYPE.to_string(),
            fields,
        }
    }
}

#[derive(Clone, Debug, Default)]
struct SyntheticCpmmSlotDelta {
    initialized: bool,
    token0_mint: Option<[u8; 32]>,
    token1_mint: Option<[u8; 32]>,
    vault0: Option<[u8; 32]>,
    vault1: Option<[u8; 32]>,
    reserve0_added: u64,
    reserve0_removed: u64,
    reserve1_added: u64,
    reserve1_removed: u64,
    total_shares_added: u128,
    total_shares_removed: u128,
    block_timestamp: u64,
}

fn cpmm_event_precedence(event_name: &str) -> usize {
    match event_name {
        "PoolInitialized" => 0,
        "AddLiquidity" => 1,
        "RemoveLiquidity" => 2,
        "Swap" => 3,
        _ => usize::MAX,
    }
}

fn accumulate_u64(target: &mut u64, value: u64, field_name: &str) -> anyhow::Result<()> {
    *target = target
        .checked_add(value)
        .ok_or_else(|| anyhow!("{} overflow while aggregating CPMM slot deltas", field_name))?;
    Ok(())
}

fn accumulate_u128(target: &mut u128, value: u128, field_name: &str) -> anyhow::Result<()> {
    *target = target
        .checked_add(value)
        .ok_or_else(|| anyhow!("{} overflow while aggregating CPMM slot deltas", field_name))?;
    Ok(())
}

fn apply_u64_delta(
    current: u64,
    added: u64,
    removed: u64,
    field_name: &str,
) -> anyhow::Result<u64> {
    let next = u128::from(current)
        .checked_add(u128::from(added))
        .ok_or_else(|| anyhow!("{} overflow while applying CPMM slot deltas", field_name))?;
    let next = next
        .checked_sub(u128::from(removed))
        .ok_or_else(|| anyhow!("{} underflow while applying CPMM slot deltas", field_name))?;
    u64::try_from(next)
        .map_err(|_| anyhow!("{} overflow while applying CPMM slot deltas", field_name))
}

fn apply_u128_delta(
    current: u128,
    added: u128,
    removed: u128,
    field_name: &str,
) -> anyhow::Result<u128> {
    let base = alloy_primitives::U256::from(current) + alloy_primitives::U256::from(added);
    let removed = alloy_primitives::U256::from(removed);
    if removed > base {
        return Err(anyhow!(
            "{} underflow while applying CPMM slot deltas",
            field_name
        ));
    }
    let next = base - removed;
    u128::try_from(next)
        .map_err(|_| anyhow!("{} overflow while applying CPMM slot deltas", field_name))
}

fn range_in_scope(
    chain: &ChainConfig,
    repair_scope: Option<&RepairScope>,
    start: u64,
    end: u64,
) -> bool {
    chain.range_in_scope(start, end)
        && repair_scope.is_none_or(|scope| scope.matches_range(start, end))
}

fn extract_u128_field(
    event: &crate::transformations::context::DecodedEvent,
    name: &str,
) -> anyhow::Result<u128> {
    match event.get_field(name)? {
        DecodedValue::Uint128(value) => Ok(*value),
        other => Err(anyhow!(
            "field '{}' in {} is not a Uint128 (got {:?})",
            name,
            event.context_info(),
            other
        )),
    }
}

fn should_rebuild_cpmm_accounts(chain: &ChainConfig, repair_scope: Option<&RepairScope>) -> bool {
    if !chain.solana_programs.contains_key(CPMM_SOURCE) {
        return false;
    }

    if let Some(scope) = repair_scope {
        if let Some(ref sources) = scope.sources {
            if !sources.contains(CPMM_SOURCE) {
                return false;
            }
        }

        if let Some(ref functions) = scope.functions {
            if !CPMM_EVENTS
                .iter()
                .any(|event_name| functions.contains(*event_name))
            {
                return false;
            }
        }
    }

    true
}

fn cpmm_range_starts(chain_name: &str) -> anyhow::Result<BTreeSet<(u64, u64)>> {
    let events_dir = decoded_solana_events_dir(chain_name);
    let mut ranges = BTreeSet::new();

    for event_name in CPMM_EVENTS {
        let dir = events_dir.join(CPMM_SOURCE).join(event_name);
        if !dir.exists() {
            continue;
        }
        for (start, end_inclusive, _) in scan_parquet_ranges(&dir)
            .with_context(|| format!("failed scanning {}", dir.display()))?
        {
            ranges.insert((start, end_inclusive + 1));
        }
    }

    Ok(ranges)
}

fn read_cpmm_events_for_range(
    chain_name: &str,
    start: u64,
    end: u64,
) -> anyhow::Result<Vec<crate::transformations::context::DecodedEvent>> {
    let events_dir = decoded_solana_events_dir(chain_name);
    let file_name = format!("{}-{}.parquet", start, end - 1);
    let mut events = Vec::new();

    for event_name in CPMM_EVENTS {
        let path = events_dir
            .join(CPMM_SOURCE)
            .join(event_name)
            .join(&file_name);
        if !path.exists() {
            continue;
        }
        events.extend(read_decoded_events_from_parquet(&path).with_context(|| {
            format!("failed reading decoded CPMM events from {}", path.display())
        })?);
    }

    events.sort_by_key(|event| {
        let tx_key = match &event.transaction_id {
            TxId::Solana(signature) => *signature,
            TxId::Evm(_) => [0u8; 64],
        };
        (
            event.block_number,
            cpmm_event_precedence(&event.event_name),
            tx_key,
            event.position.ordinal(),
        )
    });

    Ok(events)
}

pub fn rebuild_cpmm_historical_account_states(
    chain: &ChainConfig,
    repair_scope: Option<&RepairScope>,
) -> anyhow::Result<()> {
    if !should_rebuild_cpmm_accounts(chain, repair_scope) {
        return Ok(());
    }

    let owner_program: [u8; 32] = chain
        .solana_programs
        .get(CPMM_SOURCE)
        .ok_or_else(|| anyhow!("missing {} program config", CPMM_SOURCE))?
        .program_id
        .parse::<solana_sdk::pubkey::Pubkey>()
        .map_err(|_| anyhow!("invalid {} program id", CPMM_SOURCE))?
        .to_bytes();

    let output_dir = decoded_solana_accounts_dir(&chain.name)
        .join(CPMM_SOURCE)
        .join(CPMM_POOL_TYPE);
    std::fs::create_dir_all(&output_dir)
        .with_context(|| format!("failed creating {}", output_dir.display()))?;

    let desired_ranges = cpmm_range_starts(&chain.name)?;
    for (start, end_inclusive, path) in scan_parquet_ranges(&output_dir)
        .with_context(|| format!("failed scanning {}", output_dir.display()))?
    {
        let range = (start, end_inclusive + 1);
        if range_in_scope(chain, repair_scope, range.0, range.1) && !desired_ranges.contains(&range)
        {
            std::fs::remove_file(&path).with_context(|| {
                format!("failed removing stale CPMM account file {}", path.display())
            })?;
        }
    }

    let schema = build_decoded_account_state_schema();
    let mut pool_states: HashMap<[u8; 32], SyntheticCpmmPoolState> = HashMap::new();

    for (start, end) in desired_ranges {
        let emit_range = range_in_scope(chain, repair_scope, start, end);
        let output_path = output_dir.join(format!("{}-{}.parquet", start, end - 1));
        let events = read_cpmm_events_for_range(&chain.name, start, end)?;
        if events.is_empty() {
            if emit_range && output_path.exists() {
                std::fs::remove_file(&output_path).with_context(|| {
                    format!(
                        "failed removing stale CPMM account file {}",
                        output_path.display()
                    )
                })?;
            }
            continue;
        }

        let mut slot_deltas: BTreeMap<(u64, [u8; 32]), SyntheticCpmmSlotDelta> = BTreeMap::new();

        for event in events {
            let pool = match event.event_name.as_str() {
                "PoolInitialized" | "AddLiquidity" | "RemoveLiquidity" | "Swap" => {
                    event.extract_pubkey("pool")?
                }
                _ => continue,
            };
            let delta = slot_deltas
                .entry((event.block_number, pool))
                .or_insert_with(|| SyntheticCpmmSlotDelta {
                    block_timestamp: event.block_timestamp,
                    ..Default::default()
                });

            match event.event_name.as_str() {
                "PoolInitialized" => {
                    delta.initialized = true;
                    delta.token0_mint = Some(event.extract_pubkey("token0_mint")?);
                    delta.token1_mint = Some(event.extract_pubkey("token1_mint")?);
                    delta.vault0 = Some(event.extract_pubkey("vault0")?);
                    delta.vault1 = Some(event.extract_pubkey("vault1")?);
                }
                "AddLiquidity" => {
                    accumulate_u64(
                        &mut delta.reserve0_added,
                        event.extract_u64("amount0")?,
                        "reserve0",
                    )?;
                    accumulate_u64(
                        &mut delta.reserve1_added,
                        event.extract_u64("amount1")?,
                        "reserve1",
                    )?;
                    accumulate_u128(
                        &mut delta.total_shares_added,
                        extract_u128_field(&event, "shares_out")?,
                        "total_shares",
                    )?;
                }
                "RemoveLiquidity" => {
                    accumulate_u64(
                        &mut delta.reserve0_removed,
                        event.extract_u64("amount0")?,
                        "reserve0",
                    )?;
                    accumulate_u64(
                        &mut delta.reserve1_removed,
                        event.extract_u64("amount1")?,
                        "reserve1",
                    )?;
                    accumulate_u128(
                        &mut delta.total_shares_removed,
                        extract_u128_field(&event, "shares_in")?,
                        "total_shares",
                    )?;
                }
                "Swap" => {
                    let direction = event.extract_u8("direction")?;
                    let amount_in = event.extract_u64("amount_in")?;
                    let amount_out = event.extract_u64("amount_out")?;
                    let fee_dist = event.extract_u64("fee_dist")?;
                    let net_in = amount_in
                        .checked_sub(fee_dist)
                        .ok_or_else(|| anyhow!("fee_dist exceeds amount_in"))?;

                    match direction {
                        0 => {
                            accumulate_u64(&mut delta.reserve0_added, net_in, "reserve0")?;
                            accumulate_u64(&mut delta.reserve1_removed, amount_out, "reserve1")?;
                        }
                        1 => {
                            accumulate_u64(&mut delta.reserve1_added, net_in, "reserve1")?;
                            accumulate_u64(&mut delta.reserve0_removed, amount_out, "reserve0")?;
                        }
                        other => {
                            return Err(anyhow!("invalid swap direction {other} for CPMM pool"))
                        }
                    }
                }
                _ => {}
            }
        }

        let mut slot_snapshots: BTreeMap<(u64, [u8; 32]), DecodedAccountState> = BTreeMap::new();
        for ((slot, pool), delta) in slot_deltas {
            let state = match pool_states.entry(pool) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    if !delta.initialized {
                        return Err(anyhow!(
                            "CPMM pool {} mutated before PoolInitialized at slot {}",
                            bs58::encode(pool).into_string(),
                            slot
                        ));
                    }
                    entry.insert(SyntheticCpmmPoolState::default())
                }
            };

            if delta.initialized {
                state.token0_mint = delta.token0_mint;
                state.token1_mint = delta.token1_mint;
                state.vault0 = delta.vault0;
                state.vault1 = delta.vault1;
                state.reserve0 = 0;
                state.reserve1 = 0;
                state.total_shares = 0;
            }

            state.reserve0 = apply_u64_delta(
                state.reserve0,
                delta.reserve0_added,
                delta.reserve0_removed,
                "reserve0",
            )?;
            state.reserve1 = apply_u64_delta(
                state.reserve1,
                delta.reserve1_added,
                delta.reserve1_removed,
                "reserve1",
            )?;
            state.total_shares = apply_u128_delta(
                state.total_shares,
                delta.total_shares_added,
                delta.total_shares_removed,
                "total_shares",
            )?;

            slot_snapshots.insert(
                (slot, pool),
                state.to_decoded_account_state(owner_program, pool, slot, delta.block_timestamp),
            );
        }

        if !emit_range {
            continue;
        }

        let account_states: Vec<DecodedAccountState> = slot_snapshots.into_values().collect();
        if account_states.is_empty() {
            if output_path.exists() {
                std::fs::remove_file(&output_path).with_context(|| {
                    format!(
                        "failed removing stale CPMM account file {}",
                        output_path.display()
                    )
                })?;
            }
            continue;
        }

        write_decoded_account_states_to_parquet(&account_states, &schema, &output_path)
            .with_context(|| format!("failed writing {}", output_path.display()))?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::solana::decoding::decoded_parquet::read_decoded_account_states_from_parquet;
    use crate::solana::decoding::decoded_parquet::{
        build_decoded_event_schema, write_decoded_events_to_parquet,
    };
    use crate::transformations::context::DecodedEvent;
    use crate::types::chain::LogPosition;
    use alloy_primitives::U256;
    use std::sync::Mutex;
    use tempfile::TempDir;

    static TEST_CWD_LOCK: Mutex<()> = Mutex::new(());

    fn test_chain(root: &std::path::Path) -> ChainConfig {
        ChainConfig {
            name: root.file_name().unwrap().to_string_lossy().into_owned(),
            chain_id: 1,
            chain_type: crate::types::chain::ChainType::Solana,
            rpc_url_env_var: "RPC_URL".to_string(),
            ws_url_env_var: None,
            start_block: Some(U256::ZERO),
            from_block: None,
            to_block: None,
            contracts: Default::default(),
            block_receipts_method: None,
            factory_collections: Default::default(),
            rpc: Default::default(),
            solana_programs: HashMap::from([(
                CPMM_SOURCE.to_string(),
                crate::types::config::solana::SolanaProgramConfig {
                    program_id: solana_sdk::pubkey::Pubkey::new_unique().to_string(),
                    idl_path: None,
                    idl_format: None,
                    decoder: None,
                    events: None,
                    accounts: None,
                    discovery: None,
                    start_slot: None,
                },
            )]),
            commitment: crate::types::config::solana::SolanaCommitment::Confirmed,
            historical_provider: crate::types::config::solana::SolanaHistoricalProvider::Auto,
        }
    }

    fn make_event(
        slot: u64,
        event_name: &str,
        params: HashMap<Arc<str>, DecodedValue>,
    ) -> DecodedEvent {
        DecodedEvent {
            block_number: slot,
            block_timestamp: 1_700_000_000 + slot,
            transaction_id: TxId::Solana([slot as u8; 64]),
            position: LogPosition::Solana {
                instruction_index: 0,
                inner_instruction_index: None,
            },
            contract_address: ChainAddress::Solana([9; 32]),
            source_name: CPMM_SOURCE.to_string(),
            event_name: event_name.to_string(),
            event_signature: event_name.to_string(),
            params,
        }
    }

    fn write_event_group(
        chain_name: &str,
        event_name: &str,
        range_start: u64,
        range_end_inclusive: u64,
        events: &[DecodedEvent],
    ) {
        let schema = build_decoded_event_schema();
        let dir = decoded_solana_events_dir(chain_name)
            .join(CPMM_SOURCE)
            .join(event_name);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{}-{}.parquet", range_start, range_end_inclusive));
        write_decoded_events_to_parquet(events, &schema, &path).unwrap();
    }

    fn write_account_group(
        chain_name: &str,
        range_start: u64,
        range_end_inclusive: u64,
        states: &[DecodedAccountState],
    ) {
        let schema = build_decoded_account_state_schema();
        let dir = decoded_solana_accounts_dir(chain_name)
            .join(CPMM_SOURCE)
            .join(CPMM_POOL_TYPE);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{}-{}.parquet", range_start, range_end_inclusive));
        write_decoded_account_states_to_parquet(states, &schema, &path).unwrap();
    }

    #[test]
    fn rebuilds_cpmm_pool_account_states_from_decoded_events() {
        let _guard = TEST_CWD_LOCK.lock().unwrap();
        let dir = TempDir::new().unwrap();
        let previous_cwd = std::env::current_dir().unwrap();
        std::env::set_current_dir(dir.path()).unwrap();

        let chain = test_chain(dir.path());
        let pool = [7; 32];
        let token0 = [1; 32];
        let token1 = [2; 32];
        let vault0 = [3; 32];
        let vault1 = [4; 32];

        let init = make_event(
            1,
            "PoolInitialized",
            HashMap::from([
                (
                    Arc::<str>::from("pool"),
                    DecodedValue::ChainAddress(ChainAddress::Solana(pool)),
                ),
                (
                    Arc::<str>::from("token0_mint"),
                    DecodedValue::ChainAddress(ChainAddress::Solana(token0)),
                ),
                (
                    Arc::<str>::from("token1_mint"),
                    DecodedValue::ChainAddress(ChainAddress::Solana(token1)),
                ),
                (
                    Arc::<str>::from("vault0"),
                    DecodedValue::ChainAddress(ChainAddress::Solana(vault0)),
                ),
                (
                    Arc::<str>::from("vault1"),
                    DecodedValue::ChainAddress(ChainAddress::Solana(vault1)),
                ),
            ]),
        );
        let add = make_event(
            2,
            "AddLiquidity",
            HashMap::from([
                (
                    Arc::<str>::from("pool"),
                    DecodedValue::ChainAddress(ChainAddress::Solana(pool)),
                ),
                (Arc::<str>::from("amount0"), DecodedValue::Uint64(100)),
                (Arc::<str>::from("amount1"), DecodedValue::Uint64(200)),
                (Arc::<str>::from("shares_out"), DecodedValue::Uint128(10)),
            ]),
        );
        let swap = make_event(
            3,
            "Swap",
            HashMap::from([
                (
                    Arc::<str>::from("pool"),
                    DecodedValue::ChainAddress(ChainAddress::Solana(pool)),
                ),
                (Arc::<str>::from("direction"), DecodedValue::Uint8(0)),
                (Arc::<str>::from("amount_in"), DecodedValue::Uint64(50)),
                (Arc::<str>::from("amount_out"), DecodedValue::Uint64(25)),
                (Arc::<str>::from("fee_dist"), DecodedValue::Uint64(5)),
            ]),
        );

        write_event_group(&chain.name, "PoolInitialized", 0, 9, &[init]);
        write_event_group(&chain.name, "AddLiquidity", 0, 9, &[add]);
        write_event_group(&chain.name, "Swap", 0, 9, &[swap]);

        rebuild_cpmm_historical_account_states(&chain, None).unwrap();

        let account_path = decoded_solana_accounts_dir(&chain.name)
            .join(CPMM_SOURCE)
            .join(CPMM_POOL_TYPE)
            .join("0-9.parquet");
        let states = read_decoded_account_states_from_parquet(&account_path).unwrap();
        assert_eq!(states.len(), 3);
        let final_state = states.last().unwrap();
        assert_eq!(final_state.block_number, 3);
        assert_eq!(
            final_state.try_get("reserve0"),
            Some(&DecodedValue::Uint64(145))
        );
        assert_eq!(
            final_state.try_get("reserve1"),
            Some(&DecodedValue::Uint64(175))
        );
        assert_eq!(
            final_state.try_get("total_shares"),
            Some(&DecodedValue::Uint128(10))
        );
        assert_eq!(
            final_state.try_get("token0_mint"),
            Some(&DecodedValue::ChainAddress(ChainAddress::Solana(token0)))
        );

        std::env::set_current_dir(previous_cwd).unwrap();
    }

    #[test]
    fn bounded_repairs_replay_prior_ranges_to_seed_pool_state() {
        let _guard = TEST_CWD_LOCK.lock().unwrap();
        let dir = TempDir::new().unwrap();
        let previous_cwd = std::env::current_dir().unwrap();
        std::env::set_current_dir(dir.path()).unwrap();

        let chain = test_chain(dir.path());
        let pool = [7; 32];
        let token0 = [1; 32];
        let token1 = [2; 32];
        let vault0 = [3; 32];
        let vault1 = [4; 32];

        let init = make_event(
            1,
            "PoolInitialized",
            HashMap::from([
                (
                    Arc::<str>::from("pool"),
                    DecodedValue::ChainAddress(ChainAddress::Solana(pool)),
                ),
                (
                    Arc::<str>::from("token0_mint"),
                    DecodedValue::ChainAddress(ChainAddress::Solana(token0)),
                ),
                (
                    Arc::<str>::from("token1_mint"),
                    DecodedValue::ChainAddress(ChainAddress::Solana(token1)),
                ),
                (
                    Arc::<str>::from("vault0"),
                    DecodedValue::ChainAddress(ChainAddress::Solana(vault0)),
                ),
                (
                    Arc::<str>::from("vault1"),
                    DecodedValue::ChainAddress(ChainAddress::Solana(vault1)),
                ),
            ]),
        );
        let add = make_event(
            12,
            "AddLiquidity",
            HashMap::from([
                (
                    Arc::<str>::from("pool"),
                    DecodedValue::ChainAddress(ChainAddress::Solana(pool)),
                ),
                (Arc::<str>::from("amount0"), DecodedValue::Uint64(100)),
                (Arc::<str>::from("amount1"), DecodedValue::Uint64(200)),
                (Arc::<str>::from("shares_out"), DecodedValue::Uint128(10)),
            ]),
        );

        write_event_group(&chain.name, "PoolInitialized", 0, 9, &[init]);
        write_event_group(&chain.name, "AddLiquidity", 10, 19, &[add]);

        let repair_scope = RepairScope {
            from_block: Some(10),
            to_block: Some(19),
            sources: None,
            functions: None,
        };
        rebuild_cpmm_historical_account_states(&chain, Some(&repair_scope)).unwrap();

        let prior_range_path = decoded_solana_accounts_dir(&chain.name)
            .join(CPMM_SOURCE)
            .join(CPMM_POOL_TYPE)
            .join("0-9.parquet");
        assert!(
            !prior_range_path.exists(),
            "bounded repair should not emit account parquet outside the repair scope"
        );

        let repaired_range_path = decoded_solana_accounts_dir(&chain.name)
            .join(CPMM_SOURCE)
            .join(CPMM_POOL_TYPE)
            .join("10-19.parquet");
        let states = read_decoded_account_states_from_parquet(&repaired_range_path).unwrap();
        assert_eq!(states.len(), 1);
        let state = &states[0];
        assert_eq!(state.block_number, 12);
        assert_eq!(state.try_get("reserve0"), Some(&DecodedValue::Uint64(100)));
        assert_eq!(state.try_get("reserve1"), Some(&DecodedValue::Uint64(200)));
        assert_eq!(
            state.try_get("total_shares"),
            Some(&DecodedValue::Uint128(10))
        );

        std::env::set_current_dir(previous_cwd).unwrap();
    }

    #[test]
    fn same_slot_swaps_apply_as_net_deltas_across_transactions() {
        let _guard = TEST_CWD_LOCK.lock().unwrap();
        let dir = TempDir::new().unwrap();
        let previous_cwd = std::env::current_dir().unwrap();
        std::env::set_current_dir(dir.path()).unwrap();

        let chain = test_chain(dir.path());
        let pool = [7; 32];
        let token0 = [1; 32];
        let token1 = [2; 32];
        let vault0 = [3; 32];
        let vault1 = [4; 32];

        let init = make_event(
            1,
            "PoolInitialized",
            HashMap::from([
                (
                    Arc::<str>::from("pool"),
                    DecodedValue::ChainAddress(ChainAddress::Solana(pool)),
                ),
                (
                    Arc::<str>::from("token0_mint"),
                    DecodedValue::ChainAddress(ChainAddress::Solana(token0)),
                ),
                (
                    Arc::<str>::from("token1_mint"),
                    DecodedValue::ChainAddress(ChainAddress::Solana(token1)),
                ),
                (
                    Arc::<str>::from("vault0"),
                    DecodedValue::ChainAddress(ChainAddress::Solana(vault0)),
                ),
                (
                    Arc::<str>::from("vault1"),
                    DecodedValue::ChainAddress(ChainAddress::Solana(vault1)),
                ),
            ]),
        );
        let add = make_event(
            2,
            "AddLiquidity",
            HashMap::from([
                (
                    Arc::<str>::from("pool"),
                    DecodedValue::ChainAddress(ChainAddress::Solana(pool)),
                ),
                (Arc::<str>::from("amount0"), DecodedValue::Uint64(100)),
                (Arc::<str>::from("amount1"), DecodedValue::Uint64(200)),
                (Arc::<str>::from("shares_out"), DecodedValue::Uint128(10)),
            ]),
        );
        let mut swap_remove_first = make_event(
            3,
            "Swap",
            HashMap::from([
                (
                    Arc::<str>::from("pool"),
                    DecodedValue::ChainAddress(ChainAddress::Solana(pool)),
                ),
                (Arc::<str>::from("direction"), DecodedValue::Uint8(1)),
                (Arc::<str>::from("amount_in"), DecodedValue::Uint64(10)),
                (Arc::<str>::from("amount_out"), DecodedValue::Uint64(120)),
                (Arc::<str>::from("fee_dist"), DecodedValue::Uint64(0)),
            ]),
        );
        swap_remove_first.transaction_id = TxId::Solana([0; 64]);
        let mut swap_add_later = make_event(
            3,
            "Swap",
            HashMap::from([
                (
                    Arc::<str>::from("pool"),
                    DecodedValue::ChainAddress(ChainAddress::Solana(pool)),
                ),
                (Arc::<str>::from("direction"), DecodedValue::Uint8(0)),
                (Arc::<str>::from("amount_in"), DecodedValue::Uint64(50)),
                (Arc::<str>::from("amount_out"), DecodedValue::Uint64(20)),
                (Arc::<str>::from("fee_dist"), DecodedValue::Uint64(0)),
            ]),
        );
        swap_add_later.transaction_id = TxId::Solana([255; 64]);

        write_event_group(&chain.name, "PoolInitialized", 0, 9, &[init]);
        write_event_group(&chain.name, "AddLiquidity", 0, 9, &[add]);
        write_event_group(
            &chain.name,
            "Swap",
            0,
            9,
            &[swap_remove_first, swap_add_later],
        );

        rebuild_cpmm_historical_account_states(&chain, None).unwrap();

        let account_path = decoded_solana_accounts_dir(&chain.name)
            .join(CPMM_SOURCE)
            .join(CPMM_POOL_TYPE)
            .join("0-9.parquet");
        let states = read_decoded_account_states_from_parquet(&account_path).unwrap();
        let final_state = states.last().unwrap();
        assert_eq!(final_state.block_number, 3);
        assert_eq!(
            final_state.try_get("reserve0"),
            Some(&DecodedValue::Uint64(30))
        );
        assert_eq!(
            final_state.try_get("reserve1"),
            Some(&DecodedValue::Uint64(190))
        );

        std::env::set_current_dir(previous_cwd).unwrap();
    }

    #[test]
    fn rebuild_removes_stale_output_ranges_when_event_inputs_disappear() {
        let _guard = TEST_CWD_LOCK.lock().unwrap();
        let dir = TempDir::new().unwrap();
        let previous_cwd = std::env::current_dir().unwrap();
        std::env::set_current_dir(dir.path()).unwrap();

        let chain = test_chain(dir.path());
        let stale_state = DecodedAccountState {
            block_number: 25,
            block_timestamp: 1_700_000_025,
            account_address: ChainAddress::Solana([7; 32]),
            owner_program: ChainAddress::Solana([9; 32]),
            source_name: CPMM_SOURCE.to_string(),
            account_type: CPMM_POOL_TYPE.to_string(),
            fields: HashMap::new(),
        };
        write_account_group(&chain.name, 20, 29, &[stale_state]);

        let stale_path = decoded_solana_accounts_dir(&chain.name)
            .join(CPMM_SOURCE)
            .join(CPMM_POOL_TYPE)
            .join("20-29.parquet");
        assert!(stale_path.exists());

        rebuild_cpmm_historical_account_states(&chain, None).unwrap();

        assert!(
            !stale_path.exists(),
            "stale synthetic account parquet should be removed when its decoded event inputs are gone"
        );

        std::env::set_current_dir(previous_cwd).unwrap();
    }
}
