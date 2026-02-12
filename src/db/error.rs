use thiserror::Error;

#[derive(Debug, Error)]
pub enum DbError {
    #[error("Pool error: {0}")]
    PoolError(#[from] deadpool_postgres::PoolError),

    #[error("{}", format_pg_error(.0))]
    PostgresError(#[from] tokio_postgres::Error),

    #[error("Build error: {0}")]
    BuildError(#[from] deadpool_postgres::BuildError),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Migration error: {0}")]
    MigrationError(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Invalid connection string: {0}")]
    InvalidConnectionString(String),
}

fn format_pg_error(e: &tokio_postgres::Error) -> String {
    if let Some(db_err) = e.as_db_error() {
        let mut msg = format!(
            "PostgreSQL error [{}]: {}",
            db_err.code().code(),
            db_err.message()
        );
        if let Some(detail) = db_err.detail() {
            msg.push_str(&format!("\n  Detail: {}", detail));
        }
        if let Some(hint) = db_err.hint() {
            msg.push_str(&format!("\n  Hint: {}", hint));
        }
        if let Some(table) = db_err.table() {
            msg.push_str(&format!("\n  Table: {}", table));
        }
        if let Some(column) = db_err.column() {
            msg.push_str(&format!("\n  Column: {}", column));
        }
        if let Some(constraint) = db_err.constraint() {
            msg.push_str(&format!("\n  Constraint: {}", constraint));
        }
        msg
    } else {
        format!("PostgreSQL error: {}", e)
    }
}
