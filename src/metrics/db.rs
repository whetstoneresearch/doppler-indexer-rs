use std::sync::Arc;
use std::time::Duration;

use metrics::{describe_counter, describe_gauge, describe_histogram, gauge};

use crate::db::DbPool;

/// Register database metric descriptions (call once at startup).
pub fn describe_db_metrics() {
    describe_histogram!(
        "db_transaction_duration_seconds",
        "Duration of database transactions in seconds"
    );
    describe_counter!(
        "db_transactions_total",
        "Total number of database transactions"
    );
    describe_counter!(
        "db_transaction_operations_total",
        "Total SQL operations executed inside transactions"
    );
    describe_histogram!(
        "db_query_duration_seconds",
        "Duration of standalone database queries in seconds"
    );
    describe_counter!("db_queries_total", "Total number of standalone queries");
    describe_histogram!(
        "db_connection_acquire_duration_seconds",
        "Time waiting for a database pool connection in seconds"
    );
    describe_gauge!(
        "db_pool_connections_max",
        "Maximum number of connections in the pool"
    );
    describe_gauge!(
        "db_pool_connections_size",
        "Current number of connections in the pool"
    );
    describe_gauge!(
        "db_pool_connections_available",
        "Number of idle connections available in the pool"
    );
    describe_gauge!(
        "db_pool_connections_waiting",
        "Number of futures waiting for a pool connection"
    );
}

/// Periodically sample connection pool statistics and update gauges.
///
/// This spawns an infinite loop that reads pool status every `interval` and
/// updates the corresponding Prometheus gauges. It is meant to be spawned as
/// a background task via `tokio::spawn`.
pub async fn sample_pool_stats(pool: Arc<DbPool>, interval: Duration) {
    loop {
        let status = pool.pool_ref().status();

        gauge!("db_pool_connections_max").set(status.max_size as f64);
        gauge!("db_pool_connections_size").set(status.size as f64);
        gauge!("db_pool_connections_available").set(status.available as f64);
        gauge!("db_pool_connections_waiting").set(status.waiting as f64);

        tokio::time::sleep(interval).await;
    }
}
