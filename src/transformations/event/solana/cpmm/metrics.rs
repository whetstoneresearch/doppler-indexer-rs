use std::collections::HashMap;

use alloy_primitives::U256;
use async_trait::async_trait;
use bigdecimal::BigDecimal;

use crate::db::{DbOperation, DbSnapshot, DbValue};
use crate::transformations::context::{FieldExtractor, TransformationContext};
use crate::transformations::error::TransformationError;
use crate::transformations::registry::TransformationRegistry;
use crate::transformations::traits::{EventHandler, EventTrigger, TransformationHandler};
use crate::types::chain::{ChainAddress, ChainType};

pub struct CpmmMetricsHandler;

#[derive(Default)]
struct SlotAccumulator {
    price_open: Option<BigDecimal>,
    price_close: Option<BigDecimal>,
    price_high: Option<BigDecimal>,
    price_low: Option<BigDecimal>,
    volume0: u64,
    volume1: u64,
    swap_count: i32,
    liquidity_added: U256,
    liquidity_removed: U256,
    block_timestamp: u64,
}

fn build_cpmm_snapshot_upsert(
    chain_id: u64,
    handler_name: &str,
    handler_version: u32,
    pool: [u8; 32],
    slot: u64,
    block_timestamp: u64,
    price_open: Option<&BigDecimal>,
    price_close: Option<&BigDecimal>,
    price_high: Option<&BigDecimal>,
    price_low: Option<&BigDecimal>,
    active_liquidity: &str,
    volume0: u64,
    volume1: u64,
    swap_count: i32,
) -> DbOperation {
    let snapshot = Some(DbSnapshot {
        table: "pool_snapshots".to_string(),
        key_columns: vec![
            (
                "chain_id".to_string(),
                DbValue::Int64(i64::try_from(chain_id).expect("chain_id fits i64")),
            ),
            ("pool_id".to_string(), DbValue::Pubkey(pool)),
            (
                "block_height".to_string(),
                DbValue::Int64(i64::try_from(slot).expect("slot fits i64")),
            ),
            (
                "source".to_string(),
                DbValue::Text(handler_name.to_string()),
            ),
            (
                "source_version".to_string(),
                DbValue::Int32(i32::try_from(handler_version).expect("version fits i32")),
            ),
        ],
    });

    if let Some(price_open) = price_open {
        let price_close = price_close.unwrap_or(price_open);
        let price_high = price_high.unwrap_or(price_open);
        let price_low = price_low.unwrap_or(price_open);

        DbOperation::Upsert {
            table: "pool_snapshots".to_string(),
            columns: vec![
                "chain_id".into(),
                "pool_id".into(),
                "block_height".into(),
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
                DbValue::Int64(i64::try_from(chain_id).expect("chain_id fits i64")),
                DbValue::Pubkey(pool),
                DbValue::Int64(i64::try_from(slot).expect("slot fits i64")),
                DbValue::Int64(i64::try_from(block_timestamp).expect("timestamp fits i64")),
                DbValue::Numeric(price_open.to_string()),
                DbValue::Numeric(price_close.to_string()),
                DbValue::Numeric(price_high.to_string()),
                DbValue::Numeric(price_low.to_string()),
                DbValue::Numeric(active_liquidity.to_string()),
                DbValue::Numeric(volume0.to_string()),
                DbValue::Numeric(volume1.to_string()),
                DbValue::Int32(swap_count),
            ],
            conflict_columns: vec!["chain_id".into(), "pool_id".into(), "block_height".into()],
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
    } else {
        let template = r#"
WITH current_slot AS (
  SELECT price_close
  FROM pool_snapshots
  WHERE chain_id = :chain_id
    AND pool_id = :pool_id
    AND block_height = :block_height
    AND source = :source
    AND source_version = :source_version
  LIMIT 1
),
prev AS (
  SELECT price_close
  FROM pool_snapshots
  WHERE chain_id = :chain_id
    AND pool_id = :pool_id
    AND block_height < :block_height
    AND source = :source
    AND source_version = :source_version
  ORDER BY block_height DESC
  LIMIT 1
),
chosen AS (
  SELECT price_close FROM current_slot
  UNION ALL
  SELECT price_close FROM prev
  WHERE NOT EXISTS (SELECT 1 FROM current_slot)
  LIMIT 1
)
INSERT INTO pool_snapshots (
  chain_id,
  pool_id,
  block_height,
  block_timestamp,
  price_open,
  price_close,
  price_high,
  price_low,
  active_liquidity,
  volume0,
  volume1,
  swap_count,
  source,
  source_version
)
SELECT
  :chain_id,
  :pool_id,
  :block_height,
  :block_timestamp,
  chosen.price_close,
  chosen.price_close,
  chosen.price_close,
  chosen.price_close,
  :active_liquidity,
  :volume0,
  :volume1,
  :swap_count,
  :source,
  :source_version
FROM chosen
ON CONFLICT (chain_id, pool_id, block_height, source, source_version)
DO UPDATE SET
  block_timestamp = EXCLUDED.block_timestamp,
  price_open = EXCLUDED.price_open,
  price_close = EXCLUDED.price_close,
  price_high = EXCLUDED.price_high,
  price_low = EXCLUDED.price_low,
  active_liquidity = EXCLUDED.active_liquidity,
  volume0 = EXCLUDED.volume0,
  volume1 = EXCLUDED.volume1,
  swap_count = EXCLUDED.swap_count
"#;

        DbOperation::NamedSql {
            template: template.to_string(),
            params: vec![
                (
                    "chain_id".into(),
                    DbValue::Int64(i64::try_from(chain_id).expect("chain_id fits i64")),
                ),
                ("pool_id".into(), DbValue::Pubkey(pool)),
                (
                    "block_height".into(),
                    DbValue::Int64(i64::try_from(slot).expect("slot fits i64")),
                ),
                (
                    "block_timestamp".into(),
                    DbValue::Int64(i64::try_from(block_timestamp).expect("timestamp fits i64")),
                ),
                (
                    "active_liquidity".into(),
                    DbValue::Numeric(active_liquidity.to_string()),
                ),
                ("volume0".into(), DbValue::Numeric(volume0.to_string())),
                ("volume1".into(), DbValue::Numeric(volume1.to_string())),
                ("swap_count".into(), DbValue::Int32(swap_count)),
                ("source".into(), DbValue::VarChar(handler_name.to_string())),
                (
                    "source_version".into(),
                    DbValue::Int32(i32::try_from(handler_version).expect("version fits i32")),
                ),
            ],
            snapshot,
        }
    }
}

enum PoolPriceBootstrap {
    MissingPoolState,
    UnusablePoolState,
    Price(BigDecimal),
}

fn bootstrap_price_from_pool_state(
    ctx: &TransformationContext,
    pool: [u8; 32],
    slot: u64,
) -> Result<PoolPriceBootstrap, TransformationError> {
    for account_state in ctx.account_states_of_type("DopplerCPMM", "Pool") {
        if account_state.block_number != slot {
            continue;
        }
        if account_state.account_address != ChainAddress::Solana(pool) {
            continue;
        }

        let reserve0 = account_state.extract_u64("reserve0")?;
        let reserve1 = account_state.extract_u64("reserve1")?;
        if reserve0 == 0 || reserve1 == 0 {
            return Ok(PoolPriceBootstrap::UnusablePoolState);
        }

        return Ok(PoolPriceBootstrap::Price(
            BigDecimal::from(reserve1) / BigDecimal::from(reserve0),
        ));
    }

    Ok(PoolPriceBootstrap::MissingPoolState)
}

#[async_trait]
impl TransformationHandler for CpmmMetricsHandler {
    fn name(&self) -> &'static str {
        "CpmmMetricsHandler"
    }

    fn version(&self) -> u32 {
        1
    }

    fn chain_type(&self) -> ChainType {
        ChainType::Solana
    }

    fn migration_paths(&self) -> Vec<&'static str> {
        vec![
            "migrations/tables/pool_snapshots.sql",
            "migrations/tables/pool_snapshots_add_tvl.sql",
            "migrations/tables/pool_snapshots_add_volume_usd.sql",
        ]
    }

    fn reorg_tables(&self) -> Vec<&'static str> {
        vec!["pool_snapshots"]
    }

    fn requires_sequential(&self) -> bool {
        true
    }

    async fn handle(
        &self,
        ctx: &TransformationContext,
    ) -> Result<Vec<DbOperation>, TransformationError> {
        let handler_name = self.name();
        let handler_version = self.version();

        // Key: (pool_pubkey, slot)
        let mut accumulators: HashMap<([u8; 32], u64), SlotAccumulator> = HashMap::new();

        for event in ctx.events_of_type("DopplerCPMM", "Swap") {
            let pool = event.extract_pubkey("pool")?;
            let direction = event.extract_u8("direction")?;
            let amount_in = event.extract_u64("amount_in")?;
            let amount_out = event.extract_u64("amount_out")?;
            let slot = event.block_number;

            if amount_in == 0 || amount_out == 0 {
                continue;
            }

            // Express price as token1 per token0.
            // direction=0: token0 in, token1 out
            // direction=1: token1 in, token0 out
            let price = if direction == 0 {
                BigDecimal::from(amount_out) / BigDecimal::from(amount_in)
            } else {
                BigDecimal::from(amount_in) / BigDecimal::from(amount_out)
            };

            let (vol0, vol1) = if direction == 0 {
                (amount_in, amount_out)
            } else {
                (amount_out, amount_in)
            };

            let acc = accumulators
                .entry((pool, slot))
                .or_insert_with(|| SlotAccumulator {
                    block_timestamp: event.block_timestamp,
                    ..Default::default()
                });

            if acc.price_open.is_none() {
                acc.price_open = Some(price.clone());
            }
            acc.price_close = Some(price.clone());
            acc.price_high = Some(match acc.price_high.take() {
                Some(h) => h.max(price.clone()),
                None => price.clone(),
            });
            acc.price_low = Some(match acc.price_low.take() {
                Some(l) => l.min(price),
                None => price,
            });
            acc.volume0 = acc.volume0.saturating_add(vol0);
            acc.volume1 = acc.volume1.saturating_add(vol1);
            acc.swap_count += 1;
        }

        for event in ctx.events_of_type("DopplerCPMM", "AddLiquidity") {
            let pool = event.extract_pubkey("pool")?;
            let shares_out = event.extract_uint256("shares_out")?;
            let slot = event.block_number;

            let acc = accumulators
                .entry((pool, slot))
                .or_insert_with(|| SlotAccumulator {
                    block_timestamp: event.block_timestamp,
                    ..Default::default()
                });
            acc.liquidity_added = acc.liquidity_added.saturating_add(shares_out);
        }

        for event in ctx.events_of_type("DopplerCPMM", "RemoveLiquidity") {
            let pool = event.extract_pubkey("pool")?;
            let shares_in = event.extract_uint256("shares_in")?;
            let slot = event.block_number;

            let acc = accumulators
                .entry((pool, slot))
                .or_insert_with(|| SlotAccumulator {
                    block_timestamp: event.block_timestamp,
                    ..Default::default()
                });
            acc.liquidity_removed = acc.liquidity_removed.saturating_add(shares_in);
        }

        let mut ops = Vec::new();

        for ((pool, slot), acc) in accumulators {
            let bootstrap_price = if acc.price_open.is_none() {
                match bootstrap_price_from_pool_state(ctx, pool, slot)? {
                    PoolPriceBootstrap::Price(price) => Some(price),
                    PoolPriceBootstrap::UnusablePoolState => None,
                    PoolPriceBootstrap::MissingPoolState => None,
                }
            } else {
                None
            };
            let price_open = acc.price_open.as_ref().or(bootstrap_price.as_ref());
            let price_close = acc.price_close.as_ref().or(bootstrap_price.as_ref());
            let price_high = acc.price_high.as_ref().or(bootstrap_price.as_ref());
            let price_low = acc.price_low.as_ref().or(bootstrap_price.as_ref());

            // Net liquidity share delta for this slot (can be negative).
            let liquidity_delta = if acc.liquidity_added >= acc.liquidity_removed {
                (acc.liquidity_added - acc.liquidity_removed).to_string()
            } else {
                format!("-{}", acc.liquidity_removed - acc.liquidity_added)
            };

            ops.push(build_cpmm_snapshot_upsert(
                ctx.chain_id,
                handler_name,
                handler_version,
                pool,
                slot,
                acc.block_timestamp,
                price_open,
                price_close,
                price_high,
                price_low,
                &liquidity_delta,
                acc.volume0,
                acc.volume1,
                acc.swap_count,
            ));
        }

        Ok(ops)
    }
}

impl EventHandler for CpmmMetricsHandler {
    fn triggers(&self) -> Vec<EventTrigger> {
        vec![
            EventTrigger::new("DopplerCPMM", "Swap"),
            EventTrigger::new("DopplerCPMM", "AddLiquidity"),
            EventTrigger::new("DopplerCPMM", "RemoveLiquidity"),
        ]
    }

    fn account_state_dependencies(&self) -> Vec<(String, String)> {
        vec![]
    }

    fn optional_account_state_dependencies(&self) -> Vec<(String, String)> {
        vec![("DopplerCPMM".to_string(), "Pool".to_string())]
    }
}

pub fn register_handlers(registry: &mut TransformationRegistry) {
    registry.register_event_handler(CpmmMetricsHandler);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transformations::context::{
        DecodedAccountState, DecodedEvent, DecodedValue, TransformationContext,
    };
    use crate::transformations::historical::HistoricalDataReader;
    use crate::types::chain::{ChainAddress, LogPosition, TxId};
    use crate::types::config::contract::Contracts;
    use alloy::primitives::U256;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn make_context(
        events: Vec<DecodedEvent>,
        account_states: Vec<DecodedAccountState>,
    ) -> TransformationContext {
        TransformationContext::new(
            "solana-test".to_string(),
            1,
            42,
            43,
            Arc::new(events),
            Arc::new(Vec::new()),
            Arc::new(account_states),
            Arc::new(HashMap::new()),
            Arc::new(HistoricalDataReader::new("test_cpmm_metrics").expect("historical reader")),
            None,
            Some(Arc::new(Contracts::new())),
        )
    }

    fn add_liquidity_event(pool: [u8; 32], slot: u64) -> DecodedEvent {
        DecodedEvent {
            block_number: slot,
            block_timestamp: 1_700_000_000,
            transaction_id: TxId::Solana([0; 64]),
            position: LogPosition::Solana {
                instruction_index: 0,
                inner_instruction_index: None,
            },
            contract_address: ChainAddress::Solana([1; 32]),
            source_name: "DopplerCPMM".to_string(),
            event_name: "AddLiquidity".to_string(),
            event_signature: "AddLiquidity".to_string(),
            params: HashMap::from([
                (
                    Arc::from("pool"),
                    DecodedValue::ChainAddress(ChainAddress::Solana(pool)),
                ),
                (
                    Arc::from("shares_out"),
                    DecodedValue::Uint256(U256::from(5u64)),
                ),
            ]),
        }
    }

    fn pool_account_state(
        pool: [u8; 32],
        slot: u64,
        reserve0: u64,
        reserve1: u64,
    ) -> DecodedAccountState {
        DecodedAccountState {
            block_number: slot,
            block_timestamp: 1_700_000_000,
            account_address: ChainAddress::Solana(pool),
            owner_program: ChainAddress::Solana([2; 32]),
            source_name: "DopplerCPMM".to_string(),
            account_type: "Pool".to_string(),
            fields: HashMap::from([
                (Arc::from("reserve0"), DecodedValue::Uint64(reserve0)),
                (Arc::from("reserve1"), DecodedValue::Uint64(reserve1)),
            ]),
        }
    }

    #[test]
    fn liquidity_only_slots_use_previous_price_snapshot_sql() {
        let op = build_cpmm_snapshot_upsert(
            1,
            "CpmmMetricsHandler",
            1,
            [7; 32],
            42,
            1_700_000_000,
            None,
            None,
            None,
            None,
            "5",
            0,
            0,
            0,
        );

        match op {
            DbOperation::NamedSql {
                template,
                params,
                snapshot,
            } => {
                assert!(template.contains("current_slot"));
                assert!(template.contains("SELECT price_close"));
                assert!(template.contains("LIMIT 1\n),\nprev AS ("));
                assert!(template.contains("AND source = :source"));
                assert!(template.contains("AND source_version = :source_version"));
                assert!(snapshot.is_some());
                assert!(params
                    .iter()
                    .any(|(name, value)| name == "source" && matches!(value, DbValue::VarChar(_))));
            }
            other => panic!("expected NamedSql, got {:?}", other),
        }
    }

    #[test]
    fn swap_slots_still_emit_plain_snapshot_upserts() {
        let price = BigDecimal::from(3u64);

        let op = build_cpmm_snapshot_upsert(
            1,
            "CpmmMetricsHandler",
            1,
            [9; 32],
            84,
            1_700_000_001,
            Some(&price),
            Some(&price),
            Some(&price),
            Some(&price),
            "7",
            10,
            20,
            1,
        );

        match op {
            DbOperation::Upsert {
                columns,
                conflict_columns,
                ..
            } => {
                assert!(!columns.iter().any(|column| column == "source"));
                assert!(!columns.iter().any(|column| column == "source_version"));
                assert!(!conflict_columns.iter().any(|column| column == "source"));
                assert!(!conflict_columns
                    .iter()
                    .any(|column| column == "source_version"));
            }
            other => panic!("expected Upsert, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn liquidity_only_slots_without_same_slot_pool_state_keep_carry_forward_sql() {
        let pool = [7; 32];
        let ctx = make_context(vec![add_liquidity_event(pool, 42)], Vec::new());

        let ops = CpmmMetricsHandler.handle(&ctx).await.unwrap();
        assert_eq!(ops.len(), 1);
        assert!(matches!(ops.first(), Some(DbOperation::NamedSql { .. })));
    }

    #[tokio::test]
    async fn liquidity_only_slots_with_unusable_pool_state_keep_carry_forward_sql() {
        let pool = [8; 32];
        let ctx = make_context(
            vec![add_liquidity_event(pool, 42)],
            vec![pool_account_state(pool, 42, 0, 9)],
        );

        let ops = CpmmMetricsHandler.handle(&ctx).await.unwrap();
        assert_eq!(ops.len(), 1);
        assert!(matches!(ops.first(), Some(DbOperation::NamedSql { .. })));
    }
}
