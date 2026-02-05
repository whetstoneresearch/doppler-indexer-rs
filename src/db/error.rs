use thiserror::Error;

#[derive(Debug, Error)]
pub enum DbError {
    #[error("Pool error: {0}")]
    PoolError(#[from] deadpool_postgres::PoolError),

    #[error("PostgreSQL error: {0}")]
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
