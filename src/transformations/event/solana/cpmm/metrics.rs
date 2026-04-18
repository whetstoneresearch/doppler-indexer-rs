use std::collections::HashMap;

use alloy_primitives::U256;
use async_trait::async_trait;
use bigdecimal::BigDecimal;

use crate::db::{DbOperation, DbValue};
use crate::transformations::context::{FieldExtractor, TransformationContext};
use crate::transformations::error::TransformationError;
use crate::transformations::registry::TransformationRegistry;
use crate::transformations::traits::{EventHandler, EventTrigger, TransformationHandler};
use crate::types::chain::ChainType;

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
            // direction=0: user pays token1 (amount_in), receives token0 (amount_out)
            // direction=1: user pays token0 (amount_in), receives token1 (amount_out)
            let price = if direction == 0 {
                BigDecimal::from(amount_in) / BigDecimal::from(amount_out)
            } else {
                BigDecimal::from(amount_out) / BigDecimal::from(amount_in)
            };

            let (vol0, vol1) = if direction == 0 {
                (amount_out, amount_in)
            } else {
                (amount_in, amount_out)
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
        let zero = BigDecimal::from(0u64);

        for ((pool, slot), acc) in accumulators {
            let price_open = acc.price_open.unwrap_or_else(|| zero.clone());
            let price_close = acc.price_close.unwrap_or_else(|| price_open.clone());
            let price_high = acc.price_high.unwrap_or_else(|| price_open.clone());
            let price_low = acc.price_low.unwrap_or_else(|| price_open.clone());

            // Net liquidity share delta for this slot (can be negative).
            let liquidity_delta = if acc.liquidity_added >= acc.liquidity_removed {
                (acc.liquidity_added - acc.liquidity_removed).to_string()
            } else {
                format!("-{}", acc.liquidity_removed - acc.liquidity_added)
            };

            ops.push(DbOperation::Upsert {
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
                    DbValue::Int64(ctx.chain_id as i64),
                    DbValue::Pubkey(pool),
                    DbValue::Int64(slot as i64),
                    DbValue::Int64(acc.block_timestamp as i64),
                    DbValue::Numeric(price_open.to_string()),
                    DbValue::Numeric(price_close.to_string()),
                    DbValue::Numeric(price_high.to_string()),
                    DbValue::Numeric(price_low.to_string()),
                    DbValue::Numeric(liquidity_delta),
                    DbValue::Numeric(acc.volume0.to_string()),
                    DbValue::Numeric(acc.volume1.to_string()),
                    DbValue::Int32(acc.swap_count),
                ],
                conflict_columns: vec![
                    "chain_id".into(),
                    "pool_id".into(),
                    "block_height".into(),
                ],
                update_columns: vec![
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
            });
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
}

pub fn register_handlers(registry: &mut TransformationRegistry) {
    registry.register_event_handler(CpmmMetricsHandler);
}
