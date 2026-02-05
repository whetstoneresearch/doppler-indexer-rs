use std::collections::HashSet;
use std::path::Path;

use deadpool_postgres::Pool;

use super::error::DbError;

const MIGRATIONS_DIR: &str = "migrations";

pub async fn run(pool: &Pool) -> Result<(), DbError> {
    let client = pool.get().await?;

    client
        .execute(
            "CREATE TABLE IF NOT EXISTS _migrations (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL UNIQUE,
                applied_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )",
            &[],
        )
        .await?;

    let rows = client.query("SELECT name FROM _migrations", &[]).await?;
    let applied: HashSet<String> = rows.iter().map(|row| row.get(0)).collect();

    let migrations_path = Path::new(MIGRATIONS_DIR);
    if !migrations_path.exists() {
        tracing::info!("No migrations directory found, skipping migrations");
        return Ok(());
    }

    let mut entries: Vec<_> = std::fs::read_dir(migrations_path)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map(|x| x == "sql")
                .unwrap_or(false)
        })
        .collect();

    entries.sort_by_key(|e| e.file_name());

    for entry in entries {
        let name = entry.file_name().to_string_lossy().to_string();
        if !applied.contains(&name) {
            let sql = std::fs::read_to_string(entry.path())?;

            let mut client = pool.get().await?;
            let tx = client.transaction().await?;

            tx.batch_execute(&sql).await.map_err(|e| {
                DbError::MigrationError(format!("Failed to run migration {}: {}", name, e))
            })?;

            tx.execute("INSERT INTO _migrations (name) VALUES ($1)", &[&name])
                .await?;

            tx.commit().await?;

            tracing::info!("Applied migration: {}", name);
        }
    }

    tracing::info!("All migrations up to date");
    Ok(())
}
