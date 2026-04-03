use std::collections::HashSet;
use std::path::{Path, PathBuf};

use deadpool_postgres::Pool;

use super::error::DbError;
use crate::transformations::registry::TransformationRegistry;

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
        .filter(|e| e.path().extension().map(|x| x == "sql").unwrap_or(false))
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

/// Run handler-declared migrations from `migration_paths()`.
///
/// This should be called once at startup with the **unfiltered** registry so
/// that every handler's tables exist before any per-chain engine initializes.
pub async fn run_handler_migrations(
    pool: &Pool,
    registry: &TransformationRegistry,
) -> Result<(), DbError> {
    let mut migration_paths: Vec<PathBuf> = Vec::new();
    let mut seen = HashSet::new();

    for handler in registry.all_handlers() {
        for path_str in handler.migration_paths() {
            let path = PathBuf::from(path_str);
            if seen.insert(path.clone()) {
                migration_paths.push(path);
            }
        }
    }

    if migration_paths.is_empty() {
        return Ok(());
    }

    let applied: HashSet<String> = {
        let client = pool.get().await?;
        let rows = client.query("SELECT name FROM _migrations", &[]).await?;
        rows.iter().map(|r| r.get(0)).collect()
    };

    let mut sql_entries: Vec<(PathBuf, String)> = Vec::new();

    for path in &migration_paths {
        if !path.exists() {
            tracing::warn!("Handler migration path does not exist: {}", path.display());
            continue;
        }

        if path.is_file() {
            let migration_name = format!("{}", path.display());
            sql_entries.push((path.clone(), migration_name));
        } else if path.is_dir() {
            let mut dir_files: Vec<_> = std::fs::read_dir(path)?
                .filter_map(|e| e.ok())
                .filter(|e| e.path().extension().map(|x| x == "sql").unwrap_or(false))
                .collect();
            dir_files.sort_by_key(|e| e.file_name());

            for entry in dir_files {
                let file_name = entry.file_name().to_string_lossy().to_string();
                let migration_name = format!("{}", path.join(&file_name).display());
                sql_entries.push((entry.path(), migration_name));
            }
        }
    }

    for (file_path, migration_name) in &sql_entries {
        if applied.contains(migration_name) {
            continue;
        }

        let sql = std::fs::read_to_string(file_path)?;

        let mut client = pool.get().await?;
        let tx = client.transaction().await?;

        tx.batch_execute(&sql).await.map_err(|e| {
            DbError::MigrationError(format!(
                "Handler migration {} failed: {}",
                migration_name, e
            ))
        })?;

        tx.execute(
            "INSERT INTO _migrations (name) VALUES ($1)",
            &[&migration_name],
        )
        .await?;

        tx.commit().await?;

        tracing::info!("Applied handler migration: {}", migration_name);
    }

    Ok(())
}
