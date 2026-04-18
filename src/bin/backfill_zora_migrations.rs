//! One-shot backfill for Zora creator coin pool migrations.
//!
//! Reads all ZoraCreatorCoinV4:LiquidityMigrated events from the historical
//! decoded parquet files and, for each migration:
//!   1. Looks up the original pool by fromPoolKeyHash in the pools table.
//!   2. Inserts a new pool row keyed on toPoolKeyHash (source="ZoraMigrateHandler").
//!   3. Updates the original pool to record migration_pool/migrated_at/migration_type.
//!
//! Safe to re-run: inserts use ON CONFLICT DO NOTHING.
//!
//! Usage:
//!   DATABASE_URL=postgresql://... cargo run --bin backfill_zora_migrations [-- <data_dir>]
//!
//! Defaults to `data/base/historical/decoded/logs/ZoraCreatorCoinV4/LiquidityMigrated`.

use std::fs::File;
use std::path::{Path, PathBuf};

use arrow::array::{Array, FixedSizeBinaryArray, Int64Array, UInt64Array};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tokio_postgres::NoTls;

const CHAIN_ID: i64 = 8453;
const ZORA_CREATE_SOURCE: &str = "ZoraCreateHandler";
const ZORA_CREATE_VERSION: i32 = 1;
const ZORA_MIGRATE_SOURCE: &str = "ZoraMigrateHandler";
const ZORA_MIGRATE_VERSION: i32 = 1;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let data_dir = std::env::args().nth(1).unwrap_or_else(|| {
        "data/base/historical/decoded/logs/ZoraCreatorCoinV4/LiquidityMigrated".to_string()
    });

    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://doppler:doppler@localhost/doppler".to_string());

    let (client, connection) = tokio_postgres::connect(&db_url, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let mut files: Vec<PathBuf> = std::fs::read_dir(&data_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().map(|e| e == "parquet").unwrap_or(false))
        .collect();
    files.sort();

    eprintln!("found {} parquet files in {}", files.len(), data_dir);

    let mut total_rows = 0u64;
    let mut inserted = 0u64;
    let mut updated = 0u64;
    let mut skipped = 0u64;

    for file in &files {
        process_file(
            file,
            &client,
            &mut total_rows,
            &mut inserted,
            &mut updated,
            &mut skipped,
        )
        .await?;
    }

    eprintln!(
        "done: total_rows={} inserted={} updated={} skipped={}",
        total_rows, inserted, updated, skipped
    );
    Ok(())
}

async fn process_file(
    path: &Path,
    client: &tokio_postgres::Client,
    total_rows: &mut u64,
    inserted: &mut u64,
    updated: &mut u64,
    skipped: &mut u64,
) -> anyhow::Result<()> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    for batch in reader {
        let batch = batch?;
        let num_rows = batch.num_rows();
        *total_rows += num_rows as u64;

        let from_hashes = col_binary(&batch, "fromPoolKeyHash");
        let to_hashes = col_binary(&batch, "toPoolKeyHash");
        let to_currency0 = col_binary(&batch, "toPoolKey.currency0");
        let to_currency1 = col_binary(&batch, "toPoolKey.currency1");
        let to_fee = col_u64(&batch, "toPoolKey.fee");
        let to_tick_spacing = col_i64(&batch, "toPoolKey.tickSpacing");
        let to_hooks = col_binary(&batch, "toPoolKey.hooks");
        let block_numbers = col_u64(&batch, "block_number");
        let block_timestamps = col_u64(&batch, "block_timestamp");

        for i in 0..num_rows {
            let from_hash = from_hashes.value(i);
            let to_hash = to_hashes.value(i);
            let block_number = block_numbers.value(i) as i64;
            let block_timestamp = block_timestamps.value(i) as f64;

            // Look up the original pool.
            let rows = client
                .query(
                    "SELECT base_token, quote_token, is_token_0, fee, integrator, initializer \
                     FROM pools \
                     WHERE chain_id = $1 AND address = $2 AND source = $3 AND source_version = $4 \
                     LIMIT 1",
                    &[
                        &CHAIN_ID,
                        &from_hash,
                        &ZORA_CREATE_SOURCE,
                        &ZORA_CREATE_VERSION,
                    ],
                )
                .await?;

            let Some(row) = rows.first() else {
                eprintln!("  skip: no pool for from_hash={}", hex::encode(from_hash));
                *skipped += 1;
                continue;
            };

            let base_token: Vec<u8> = row.get("base_token");
            let quote_token: Vec<u8> = row.get("quote_token");
            let is_token_0: bool = row.get("is_token_0");
            let fee: i32 = row.get("fee");
            let integrator: Vec<u8> = row.get("integrator");
            let initializer: Vec<u8> = row.get("initializer");

            // Build pool_key JSON.
            let pool_key_json = serde_json::json!({
                "currency0": format!("0x{}", hex::encode(to_currency0.value(i))),
                "currency1": format!("0x{}", hex::encode(to_currency1.value(i))),
                "fee": to_fee.value(i) as u32,
                "tick_spacing": to_tick_spacing.value(i) as i32,
                "hooks": format!("0x{}", hex::encode(to_hooks.value(i))),
            });

            // Insert new migrated pool (ON CONFLICT DO NOTHING → idempotent).
            let n = client
                .execute(
                    "INSERT INTO pools (
                        chain_id, block_number, created_at, address,
                        base_token, quote_token, is_token_0, type,
                        integrator, initializer, fee,
                        min_threshold, max_threshold,
                        migrator, migrated_at, migration_pool, migration_type,
                        lock_duration, beneficiaries, pool_key,
                        starting_time, ending_time,
                        source, source_version
                     ) VALUES (
                        $1, $2, to_timestamp($3), $4,
                        $5, $6, $7, 'zora_creator_coin',
                        $8, $9, $10,
                        0, 0,
                        '\\x0000000000000000000000000000000000000000', NULL,
                        '\\x0000000000000000000000000000000000000000000000000000000000000000',
                        'unknown',
                        NULL, NULL, $11::jsonb,
                        to_timestamp(0), to_timestamp(0),
                        $12, $13
                     )
                     ON CONFLICT (chain_id, address, source, source_version) DO NOTHING",
                    &[
                        &CHAIN_ID,
                        &block_number,
                        &block_timestamp,
                        &to_hash,
                        &base_token,
                        &quote_token,
                        &is_token_0,
                        &integrator,
                        &initializer,
                        &fee,
                        &pool_key_json,
                        &ZORA_MIGRATE_SOURCE,
                        &ZORA_MIGRATE_VERSION,
                    ],
                )
                .await?;
            *inserted += n;

            // Update original pool with migration metadata.
            let n = client
                .execute(
                    "UPDATE pools \
                     SET migration_pool = $1, migrated_at = to_timestamp($2), migration_type = 'zora' \
                     WHERE chain_id = $3 AND address = $4 AND source = $5 AND source_version = $6",
                    &[
                        &to_hash,
                        &block_timestamp,
                        &CHAIN_ID,
                        &from_hash,
                        &ZORA_CREATE_SOURCE,
                        &ZORA_CREATE_VERSION,
                    ],
                )
                .await?;
            *updated += n;
        }
    }

    Ok(())
}

fn col_binary<'a>(
    batch: &'a arrow::record_batch::RecordBatch,
    name: &str,
) -> &'a FixedSizeBinaryArray {
    let idx = batch
        .schema()
        .index_of(name)
        .unwrap_or_else(|_| panic!("column '{}' not found in parquet schema", name));
    let col = batch.column(idx);
    col.as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap_or_else(|| {
            panic!(
                "column '{}' is not FixedSizeBinaryArray (type={:?})",
                name,
                col.data_type()
            )
        })
}

fn col_u64<'a>(batch: &'a arrow::record_batch::RecordBatch, name: &str) -> &'a UInt64Array {
    let idx = batch
        .schema()
        .index_of(name)
        .unwrap_or_else(|_| panic!("column '{}' not found in parquet schema", name));
    let col = batch.column(idx);
    // ubigint in DuckDB parquet maps to UInt64 in arrow.
    col.as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap_or_else(|| {
            panic!(
                "column '{}' is not UInt64Array (type={:?})",
                name,
                col.data_type()
            )
        })
}

fn col_i64<'a>(batch: &'a arrow::record_batch::RecordBatch, name: &str) -> &'a Int64Array {
    let idx = batch
        .schema()
        .index_of(name)
        .unwrap_or_else(|_| panic!("column '{}' not found in parquet schema", name));
    let col = batch.column(idx);
    col.as_any()
        .downcast_ref::<Int64Array>()
        .unwrap_or_else(|| {
            panic!(
                "column '{}' is not Int64Array (type={:?})",
                name,
                col.data_type()
            )
        })
}
