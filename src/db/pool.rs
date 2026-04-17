use std::time::Instant;

use bytes::BytesMut;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod, Runtime};
use metrics::{counter, histogram};
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use rand::Rng;
use tokio_postgres::config::SslMode;
use tokio_postgres::error::SqlState;
use tokio_postgres::types::ToSql;
use tokio_postgres::NoTls;

use super::error::DbError;
use super::types::{DbOperation, DbValue, WhereClause};

/// Maximum number of retries for deadlock errors (40P01).
const DEADLOCK_MAX_RETRIES: u32 = 3;
/// Base delay between deadlock retries (doubled each attempt).
const DEADLOCK_BASE_DELAY_MS: u64 = 50;

pub struct DbPool {
    pool: Pool,
}

impl DbPool {
    pub async fn new(database_url: &str, pool_size: usize) -> Result<Self, DbError> {
        let config = database_url
            .parse::<tokio_postgres::Config>()
            .map_err(|e| DbError::InvalidConnectionString(e.to_string()))?;

        let manager_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };

        let ssl_mode = config.get_ssl_mode();
        let manager = match ssl_mode {
            SslMode::Disable => {
                tracing::info!("Database TLS disabled by connection string (sslmode=disable)");
                Manager::from_config(config, NoTls, manager_config)
            }
            _ => {
                let tls_connector = build_tls_connector()?;
                tracing::info!(ssl_mode = ?ssl_mode, "Database TLS enabled");
                Manager::from_config(config, tls_connector, manager_config)
            }
        };

        let pool = Pool::builder(manager)
            .max_size(pool_size)
            .runtime(Runtime::Tokio1)
            .build()
            .map_err(DbError::BuildError)?;

        let _conn = pool.get().await?;
        tracing::info!(
            "Database connection pool created successfully (size={})",
            pool_size
        );

        Ok(Self { pool })
    }

    pub fn inner(&self) -> &Pool {
        &self.pool
    }

    /// Return a reference to the underlying deadpool pool.
    ///
    /// Used by the metrics sampler to call `.status()` for pool gauge updates.
    pub fn pool_ref(&self) -> &Pool {
        &self.pool
    }

    pub async fn execute_transaction(&self, operations: Vec<DbOperation>) -> Result<(), DbError> {
        if operations.is_empty() {
            return Ok(());
        }

        let mut operations = operations;
        sort_operations_for_lock_ordering(&mut operations);

        let op_count = operations.len();
        let txn_start = Instant::now();

        counter!(
            "db_transaction_operations_total",
            "method" => "execute_transaction"
        )
        .increment(op_count as u64);

        for attempt in 0..=DEADLOCK_MAX_RETRIES {
            match self.try_execute_transaction(&operations).await {
                Ok(()) => {
                    histogram!(
                        "db_transaction_duration_seconds",
                        "method" => "execute_transaction",
                        "status" => "success"
                    )
                    .record(txn_start.elapsed().as_secs_f64());
                    counter!(
                        "db_transactions_total",
                        "method" => "execute_transaction",
                        "status" => "success"
                    )
                    .increment(1);
                    return Ok(());
                }
                Err(e) if is_deadlock(&e) && attempt < DEADLOCK_MAX_RETRIES => {
                    let base_delay_ms = DEADLOCK_BASE_DELAY_MS * (1u64 << attempt);
                    let jitter_range = base_delay_ms / 5;
                    let jitter_offset = rand::rng().random_range(0..=(jitter_range * 2));
                    let delay_ms = base_delay_ms - jitter_range + jitter_offset;
                    tracing::warn!(
                        "Deadlock detected (attempt {}/{}), retrying in {}ms (base={}ms)",
                        attempt + 1,
                        DEADLOCK_MAX_RETRIES + 1,
                        delay_ms,
                        base_delay_ms,
                    );
                    tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                }
                Err(e) => {
                    histogram!(
                        "db_transaction_duration_seconds",
                        "method" => "execute_transaction",
                        "status" => "error"
                    )
                    .record(txn_start.elapsed().as_secs_f64());
                    counter!(
                        "db_transactions_total",
                        "method" => "execute_transaction",
                        "status" => "error"
                    )
                    .increment(1);
                    return Err(e);
                }
            }
        }

        unreachable!()
    }

    async fn try_execute_transaction(&self, operations: &[DbOperation]) -> Result<(), DbError> {
        let acquire_start = Instant::now();
        let mut client = match self.pool.get().await {
            Ok(c) => {
                histogram!("db_connection_acquire_duration_seconds")
                    .record(acquire_start.elapsed().as_secs_f64());
                c
            }
            Err(e) => {
                histogram!("db_connection_acquire_duration_seconds")
                    .record(acquire_start.elapsed().as_secs_f64());
                return Err(e.into());
            }
        };

        let transaction = client.transaction().await?;

        for op in operations {
            let (sql, params) = build_operation_sql(op);

            let params_refs: Vec<&(dyn ToSql + Sync)> =
                params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();

            if let Err(e) = transaction.execute(&sql, &params_refs[..]).await {
                let db_err: DbError = e.into();
                tracing::error!("SQL execution failed\n  SQL: {}\n  Error: {}", sql, db_err);
                return Err(db_err);
            }
        }

        transaction.commit().await?;
        Ok(())
    }

    /// Execute a transaction with pre-queries that run inside the same transaction context.
    /// Snapshot queries see committed state before any modifications in this transaction.
    /// Returns a tuple of (snapshot_results, affected_rows) after the transaction commits.
    /// `affected_rows[i]` is the number of rows touched by `operations[i]`.
    pub async fn execute_transaction_with_snapshot_reads(
        &self,
        snapshot_specs: &[(String, Vec<(String, DbValue)>)],
        operations: Vec<DbOperation>,
    ) -> Result<(Vec<Option<Vec<(String, DbValue)>>>, Vec<u64>), DbError> {
        if operations.is_empty() && snapshot_specs.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }

        let op_count = operations.len();

        let acquire_start = Instant::now();
        let mut client = match self.pool.get().await {
            Ok(c) => {
                histogram!("db_connection_acquire_duration_seconds")
                    .record(acquire_start.elapsed().as_secs_f64());
                c
            }
            Err(e) => {
                histogram!("db_connection_acquire_duration_seconds")
                    .record(acquire_start.elapsed().as_secs_f64());
                counter!(
                    "db_transactions_total",
                    "method" => "execute_transaction_with_snapshot_reads",
                    "status" => "error"
                )
                .increment(1);
                return Err(e.into());
            }
        };

        let txn_start = Instant::now();
        let transaction = client.transaction().await?;

        counter!(
            "db_transaction_operations_total",
            "method" => "execute_transaction_with_snapshot_reads"
        )
        .increment(op_count as u64);

        // Run snapshot queries inside the transaction (sees state before our modifications)
        let mut snapshot_results = Vec::new();
        for (table, key_columns) in snapshot_specs {
            let result = query_row_on_transaction(&transaction, table, key_columns).await?;
            snapshot_results.push(result);
        }

        // Execute all operations and collect affected row counts
        let mut affected_rows = Vec::with_capacity(operations.len());
        for op in operations {
            let (sql, params) = build_operation_sql(&op);
            let params_refs: Vec<&(dyn ToSql + Sync)> =
                params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();

            match transaction.execute(&sql, &params_refs[..]).await {
                Ok(n) => affected_rows.push(n),
                Err(e) => {
                    let db_err: DbError = e.into();
                    tracing::error!("SQL execution failed\n  SQL: {}\n  Error: {}", sql, db_err);
                    histogram!(
                        "db_transaction_duration_seconds",
                        "method" => "execute_transaction_with_snapshot_reads",
                        "status" => "error"
                    )
                    .record(txn_start.elapsed().as_secs_f64());
                    counter!(
                        "db_transactions_total",
                        "method" => "execute_transaction_with_snapshot_reads",
                        "status" => "error"
                    )
                    .increment(1);
                    return Err(db_err);
                }
            }
        }

        transaction.commit().await?;

        histogram!(
            "db_transaction_duration_seconds",
            "method" => "execute_transaction_with_snapshot_reads",
            "status" => "success"
        )
        .record(txn_start.elapsed().as_secs_f64());
        counter!(
            "db_transactions_total",
            "method" => "execute_transaction_with_snapshot_reads",
            "status" => "success"
        )
        .increment(1);

        Ok((snapshot_results, affected_rows))
    }

    pub async fn run_migrations(&self) -> Result<(), DbError> {
        super::migrations::run(&self.pool).await
    }

    pub async fn run_handler_migrations(
        &self,
        registry: &crate::transformations::registry::TransformationRegistry,
    ) -> Result<(), DbError> {
        super::migrations::run_handler_migrations(&self.pool, registry).await
    }

    pub async fn query(
        &self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<tokio_postgres::Row>, DbError> {
        let acquire_start = Instant::now();
        let client = match self.pool.get().await {
            Ok(c) => {
                histogram!("db_connection_acquire_duration_seconds")
                    .record(acquire_start.elapsed().as_secs_f64());
                c
            }
            Err(e) => {
                histogram!("db_connection_acquire_duration_seconds")
                    .record(acquire_start.elapsed().as_secs_f64());
                counter!("db_queries_total", "status" => "error").increment(1);
                return Err(e.into());
            }
        };

        let query_start = Instant::now();
        match client.query(query, params).await {
            Ok(rows) => {
                histogram!("db_query_duration_seconds").record(query_start.elapsed().as_secs_f64());
                counter!("db_queries_total", "status" => "success").increment(1);
                Ok(rows)
            }
            Err(e) => {
                histogram!("db_query_duration_seconds").record(query_start.elapsed().as_secs_f64());
                counter!("db_queries_total", "status" => "error").increment(1);
                Err(e.into())
            }
        }
    }

    /// Query a single row by key columns.
    /// Returns the row as a list of (column_name, value) pairs if found.
    #[allow(dead_code)]
    pub async fn query_row(
        &self,
        table: &str,
        key_columns: &[(String, DbValue)],
    ) -> Result<Option<Vec<(String, DbValue)>>, DbError> {
        if key_columns.is_empty() {
            return Ok(None);
        }

        let (sql, params) = build_query_row_sql(table, key_columns);
        let params_refs: Vec<&(dyn ToSql + Sync)> =
            params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();

        let client = self.pool.get().await?;
        let rows = client.query(&sql, &params_refs[..]).await?;

        extract_row_result(&rows)
    }
}

fn build_tls_connector() -> Result<MakeTlsConnector, DbError> {
    let tls_connector = TlsConnector::builder()
        .build()
        .map_err(|e| DbError::ConfigError(format!("failed to build database TLS connector: {e}")))?;

    Ok(MakeTlsConnector::new(tls_connector))
}

/// Check whether a `DbError` is a PostgreSQL deadlock (SQLSTATE 40P01).
fn is_deadlock(err: &DbError) -> bool {
    match err {
        DbError::PostgresError(e) => e
            .as_db_error()
            .is_some_and(|db| *db.code() == SqlState::T_R_DEADLOCK_DETECTED),
        _ => false,
    }
}

/// Build SQL and parameters from a DbOperation.
fn build_operation_sql(op: &DbOperation) -> (String, Vec<SqlParam>) {
    match op {
        DbOperation::Upsert {
            table,
            columns,
            values,
            conflict_columns,
            update_columns,
            update_condition,
        } => build_upsert_sql(
            table,
            columns,
            values,
            conflict_columns,
            update_columns,
            update_condition.as_deref(),
        ),
        DbOperation::Insert {
            table,
            columns,
            values,
        } => build_insert_sql(table, columns, values),
        DbOperation::Update {
            table,
            set_columns,
            where_clause,
        } => build_update_sql(table, set_columns, where_clause),
        DbOperation::Delete {
            table,
            where_clause,
        } => build_delete_sql(table, where_clause),
        DbOperation::RawSql { query, params, .. } => {
            (query.clone(), convert_values_to_params(params))
        }
        DbOperation::NamedSql {
            template, params, ..
        } => build_named_sql(template, params),
    }
}

/// Build the SQL and parameters for a single-row query by key columns.
fn build_query_row_sql(table: &str, key_columns: &[(String, DbValue)]) -> (String, Vec<SqlParam>) {
    let mut where_parts = Vec::new();
    let mut params = Vec::new();
    for (i, (col, val)) in key_columns.iter().enumerate() {
        let placeholder = placeholder_for(val, i + 1);
        where_parts.push(format!("{} = {}", quote_ident(col), placeholder));
        params.push(convert_db_value(val));
    }

    let sql = format!(
        "SELECT * FROM {} WHERE {} LIMIT 1",
        table,
        where_parts.join(" AND ")
    );

    (sql, params)
}

/// Extract column names and values from the first row of a query result.
fn extract_row_result(
    rows: &[tokio_postgres::Row],
) -> Result<Option<Vec<(String, DbValue)>>, DbError> {
    if rows.is_empty() {
        return Ok(None);
    }

    let row = &rows[0];
    let mut result = Vec::new();
    for col in row.columns() {
        let col_name = col.name().to_string();
        let db_value = extract_db_value_from_row(row, col)?;
        result.push((col_name, db_value));
    }

    Ok(Some(result))
}

/// Query a single row by key columns using an existing transaction.
/// Returns the row as a list of (column_name, value) pairs if found.
async fn query_row_on_transaction(
    txn: &tokio_postgres::Transaction<'_>,
    table: &str,
    key_columns: &[(String, DbValue)],
) -> Result<Option<Vec<(String, DbValue)>>, DbError> {
    if key_columns.is_empty() {
        return Ok(None);
    }

    let (sql, params) = build_query_row_sql(table, key_columns);
    let params_refs: Vec<&(dyn ToSql + Sync)> =
        params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();

    let rows = txn.query(&sql, &params_refs[..]).await?;

    extract_row_result(&rows)
}

/// Try to extract a typed value from a row column, returning `None` on
/// `None` or error. Used by `extract_db_value_from_row` to collapse the
/// repetitive `Ok(Some(v)) | Ok(None) | Err(_)` pattern.
fn try_extract_column<T: tokio_postgres::types::FromSqlOwned>(
    row: &tokio_postgres::Row,
    col_name: &str,
) -> Option<T> {
    match row.try_get::<_, Option<T>>(col_name) {
        Ok(Some(v)) => Some(v),
        _ => None,
    }
}

/// Extract a DbValue from a tokio_postgres row column.
fn extract_db_value_from_row(
    row: &tokio_postgres::Row,
    col: &tokio_postgres::Column,
) -> Result<DbValue, DbError> {
    use tokio_postgres::types::Type;

    let col_name = col.name();
    let col_type = col.type_();

    match *col_type {
        Type::BOOL => Ok(try_extract_column::<bool>(row, col_name)
            .map(DbValue::Bool)
            .unwrap_or(DbValue::Null)),
        Type::INT8 => Ok(try_extract_column::<i64>(row, col_name)
            .map(DbValue::Int64)
            .unwrap_or(DbValue::Null)),
        Type::INT4 => Ok(try_extract_column::<i32>(row, col_name)
            .map(DbValue::Int32)
            .unwrap_or(DbValue::Null)),
        Type::INT2 => Ok(try_extract_column::<i16>(row, col_name)
            .map(DbValue::Int2)
            .unwrap_or(DbValue::Null)),
        Type::FLOAT8 => Ok(try_extract_column::<f64>(row, col_name)
            .map(DbValue::Float64)
            .unwrap_or(DbValue::Null)),
        Type::TEXT | Type::VARCHAR => Ok(try_extract_column::<String>(row, col_name)
            .map(DbValue::Text)
            .unwrap_or(DbValue::Null)),
        Type::BYTEA => Ok(try_extract_column::<Vec<u8>>(row, col_name)
            .map(DbValue::Bytes)
            .unwrap_or(DbValue::Null)),
        Type::NUMERIC => {
            // NUMERIC comes back as a rust_decimal::Decimal
            match row.try_get::<_, Option<rust_decimal::Decimal>>(col_name) {
                Ok(Some(v)) => Ok(DbValue::Numeric(v.to_string())),
                Ok(None) => Ok(DbValue::Null),
                Err(_) => {
                    // Fallback: try as string
                    match row.try_get::<_, Option<String>>(col_name) {
                        Ok(Some(v)) => Ok(DbValue::Numeric(v)),
                        _ => Ok(DbValue::Null),
                    }
                }
            }
        }
        Type::TIMESTAMP | Type::TIMESTAMPTZ => {
            // Try to get as system time and convert to unix timestamp
            match row.try_get::<_, Option<std::time::SystemTime>>(col_name) {
                Ok(Some(v)) => {
                    let unix = v
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs() as i64)
                        .unwrap_or(0);
                    Ok(DbValue::Timestamp(unix))
                }
                Ok(None) => Ok(DbValue::Null),
                Err(_) => Ok(DbValue::Null),
            }
        }
        Type::JSON | Type::JSONB => Ok(try_extract_column::<serde_json::Value>(row, col_name)
            .map(DbValue::JsonB)
            .unwrap_or(DbValue::Null)),
        _ => {
            // Unknown type - try as bytes, then text, then null
            if let Ok(Some(v)) = row.try_get::<_, Option<Vec<u8>>>(col_name) {
                Ok(DbValue::Bytes(v))
            } else if let Ok(Some(v)) = row.try_get::<_, Option<String>>(col_name) {
                Ok(DbValue::Text(v))
            } else {
                Ok(DbValue::Null)
            }
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
enum SqlParam {
    Null,
    Bool(bool),
    Int64(i64),
    Int32(i32),
    Int16(i16),
    Float64(f64),
    Text(String),
    Bytes(Vec<u8>),
    Json(serde_json::Value),
}

impl ToSql for SqlParam {
    fn to_sql(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self {
            SqlParam::Null => Ok(tokio_postgres::types::IsNull::Yes),
            SqlParam::Bool(v) => v.to_sql(ty, out),
            SqlParam::Int64(v) => v.to_sql(ty, out),
            SqlParam::Int32(v) => v.to_sql(ty, out),
            SqlParam::Int16(v) => v.to_sql(ty, out),
            SqlParam::Float64(v) => v.to_sql(ty, out),
            SqlParam::Text(v) => v.to_sql(ty, out),
            SqlParam::Bytes(v) => v.to_sql(ty, out),
            SqlParam::Json(v) => v.to_sql(ty, out),
        }
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        <bool as ToSql>::accepts(ty)
            || <i64 as ToSql>::accepts(ty)
            || <i32 as ToSql>::accepts(ty)
            || <i16 as ToSql>::accepts(ty)
            || <f64 as ToSql>::accepts(ty)
            || <String as ToSql>::accepts(ty)
            || <Vec<u8> as ToSql>::accepts(ty)
            || <serde_json::Value as ToSql>::accepts(ty)
    }

    fn to_sql_checked(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        // Null is valid for any column type
        if matches!(self, SqlParam::Null) {
            return Ok(tokio_postgres::types::IsNull::Yes);
        }
        if !<Self as ToSql>::accepts(ty) {
            return Err(format!("cannot convert SqlParam to PostgreSQL type {}", ty).into());
        }
        self.to_sql(ty, out)
    }
}

fn convert_db_value(value: &DbValue) -> SqlParam {
    match value {
        DbValue::Null => SqlParam::Null,
        DbValue::Bool(v) => SqlParam::Bool(*v),
        DbValue::Int64(v) => SqlParam::Int64(*v),
        DbValue::Int32(v) => SqlParam::Int32(*v),
        DbValue::Int2(v) => SqlParam::Int16(*v),
        DbValue::Uint64(v) => SqlParam::Int64(*v as i64),
        DbValue::Text(v) => SqlParam::Text(v.clone()),
        DbValue::VarChar(v) => SqlParam::Text(v.clone()),
        DbValue::Bytes(v) => SqlParam::Bytes(v.clone()),
        DbValue::Address(v) => SqlParam::Bytes(v.to_vec()),
        DbValue::Pubkey(v) => SqlParam::Bytes(v.to_vec()),
        DbValue::Bytes32(v) => SqlParam::Bytes(v.to_vec()),
        DbValue::Numeric(v) => SqlParam::Text(v.clone()),
        DbValue::Timestamp(v) => SqlParam::Float64(*v as f64),
        DbValue::Json(v) => SqlParam::Json(v.clone()),
        DbValue::JsonB(v) => SqlParam::Json(v.clone()),
        DbValue::Float64(v) => SqlParam::Float64(*v),
    }
}

fn convert_values_to_params(values: &[DbValue]) -> Vec<SqlParam> {
    values.iter().map(convert_db_value).collect()
}

/// Generate the SQL placeholder for a value at the given parameter index.
/// Uses casts for types that need special handling:
/// - Timestamp → `to_timestamp($N)`
/// - Numeric → `$N::numeric` (sent as text, cast by PostgreSQL)
fn placeholder_for(value: &DbValue, param_idx: usize) -> String {
    match value {
        DbValue::Timestamp(_) => format!("to_timestamp(${})", param_idx),
        DbValue::Numeric(_) => format!("${}::text::numeric", param_idx),
        DbValue::JsonB(_) => format!("${}::jsonb", param_idx),
        _ => format!("${}", param_idx),
    }
}

// ─── Named-parameter SQL expansion ─────────────────────────────────

/// Walk a SQL template and replace `:param_name` placeholders by calling
/// `resolve(name)` for each. Handles `::` (PostgreSQL cast operator) and
/// single/double-quoted string literals without false matches.
fn expand_named_template(template: &str, mut resolve: impl FnMut(&str) -> String) -> String {
    let chars: Vec<char> = template.chars().collect();
    let len = chars.len();
    let mut out = String::with_capacity(template.len());
    let mut i = 0;

    #[derive(PartialEq)]
    enum State {
        Normal,
        InSingleQuote,
        InDoubleQuote,
    }
    let mut state = State::Normal;

    while i < len {
        let ch = chars[i];
        match state {
            State::InSingleQuote => {
                out.push(ch);
                if ch == '\'' {
                    // '' is an escaped quote inside a string literal
                    if i + 1 < len && chars[i + 1] == '\'' {
                        out.push('\'');
                        i += 2;
                    } else {
                        state = State::Normal;
                        i += 1;
                    }
                } else {
                    i += 1;
                }
            }
            State::InDoubleQuote => {
                out.push(ch);
                if ch == '"' {
                    if i + 1 < len && chars[i + 1] == '"' {
                        out.push('"');
                        i += 2;
                    } else {
                        state = State::Normal;
                        i += 1;
                    }
                } else {
                    i += 1;
                }
            }
            State::Normal => {
                if ch == '\'' {
                    state = State::InSingleQuote;
                    out.push(ch);
                    i += 1;
                } else if ch == '"' {
                    state = State::InDoubleQuote;
                    out.push(ch);
                    i += 1;
                } else if ch == ':' {
                    if i + 1 < len && chars[i + 1] == ':' {
                        // PostgreSQL cast operator — emit literally
                        out.push(':');
                        out.push(':');
                        i += 2;
                    } else if i + 1 < len
                        && (chars[i + 1].is_ascii_alphabetic() || chars[i + 1] == '_')
                    {
                        // Named parameter — capture name
                        let start = i + 1;
                        let mut end = start;
                        while end < len && (chars[end].is_ascii_alphanumeric() || chars[end] == '_')
                        {
                            end += 1;
                        }
                        let name: String = chars[start..end].iter().collect();
                        out.push_str(&resolve(&name));
                        i = end;
                    } else {
                        out.push(ch);
                        i += 1;
                    }
                } else {
                    out.push(ch);
                    i += 1;
                }
            }
        }
    }
    out
}

/// Build SQL + SqlParam vec from a named-parameter template and DbValue params.
///
/// Each `:name` is replaced with `placeholder_for(value, idx)` which auto-adds
/// type-specific casts (`::text::numeric`, `to_timestamp()`, `::jsonb`).
/// Same `:name` used multiple times reuses the same `$N` index.
fn build_named_sql(template: &str, named_params: &[(String, DbValue)]) -> (String, Vec<SqlParam>) {
    use std::collections::HashMap;

    let lookup: HashMap<&str, &DbValue> =
        named_params.iter().map(|(k, v)| (k.as_str(), v)).collect();
    let mut index_map: HashMap<String, usize> = HashMap::new();
    let mut sql_params: Vec<SqlParam> = Vec::new();

    let sql = expand_named_template(template, |name| {
        if let Some(&idx) = index_map.get(name) {
            // Already assigned — reuse the same $N with the same cast
            let value = lookup.get(name).expect("param was previously resolved");
            placeholder_for(value, idx)
        } else {
            let value = *lookup.get(name).unwrap_or_else(|| {
                panic!(
                    "NamedSql: template references unbound parameter ':{}'",
                    name
                )
            });
            let idx = sql_params.len() + 1; // 1-based PostgreSQL param index
            index_map.insert(name.to_string(), idx);
            sql_params.push(convert_db_value(value));
            placeholder_for(value, idx)
        }
    });

    (sql, sql_params)
}

/// Reusable named-parameter builder for direct `client.query()`/`execute()` calls.
///
/// Uses the same parser as `NamedSql` but works with `&(dyn ToSql + Sync)` params
/// directly, without `DbValue` wrapping. Type casts must be added in the template
/// where needed (e.g. `:min_liquidity::bigint`).
pub struct NamedQueryBuilder<'a> {
    template: &'a str,
    params: Vec<(&'a str, &'a (dyn ToSql + Sync))>,
}

impl<'a> NamedQueryBuilder<'a> {
    pub fn new(template: &'a str) -> Self {
        Self {
            template,
            params: Vec::new(),
        }
    }

    pub fn bind(mut self, name: &'a str, value: &'a (dyn ToSql + Sync)) -> Self {
        self.params.push((name, value));
        self
    }

    /// Expand the template and return (sql, ordered_params).
    ///
    /// Panics if the template references a name not provided via `bind()`.
    pub fn build(&self) -> (String, Vec<&'a (dyn ToSql + Sync)>) {
        use std::collections::HashMap;

        let lookup: HashMap<&str, &'a (dyn ToSql + Sync)> = self.params.iter().copied().collect();
        let mut index_map: HashMap<String, usize> = HashMap::new();
        let mut ordered: Vec<&'a (dyn ToSql + Sync)> = Vec::new();

        let sql = expand_named_template(self.template, |name| {
            if let Some(&idx) = index_map.get(name) {
                format!("${}", idx)
            } else {
                let value = *lookup.get(name).unwrap_or_else(|| {
                    panic!(
                        "NamedQueryBuilder: template references unbound parameter ':{}'",
                        name
                    )
                });
                let idx = ordered.len() + 1;
                index_map.insert(name.to_string(), idx);
                ordered.push(value);
                format!("${}", idx)
            }
        });

        (sql, ordered)
    }
}

/// Wrap a column name in double quotes to handle reserved keywords.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name)
}

fn quote_cols(columns: &[String]) -> String {
    columns
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Builder for SQL queries that manages parameter indexing automatically.
///
/// This eliminates the need to manually track `param_idx` across multiple
/// calls and provides a unified interface for adding parameters.
struct QueryBuilder {
    params: Vec<SqlParam>,
}

impl QueryBuilder {
    fn new() -> Self {
        Self { params: Vec::new() }
    }

    /// Returns the next parameter index (1-based).
    fn next_idx(&self) -> usize {
        self.params.len() + 1
    }

    /// Add a value and return its placeholder string.
    fn add(&mut self, value: &DbValue) -> String {
        let idx = self.next_idx();
        let placeholder = placeholder_for(value, idx);
        self.params.push(convert_db_value(value));
        placeholder
    }

    /// Add multiple values and return their placeholder strings joined by separator.
    fn add_values(&mut self, values: &[DbValue], sep: &str) -> String {
        values
            .iter()
            .map(|v| self.add(v))
            .collect::<Vec<_>>()
            .join(sep)
    }

    /// Build a SET clause for UPDATE statements.
    fn add_set_clause(&mut self, columns: &[(String, DbValue)]) -> String {
        columns
            .iter()
            .map(|(col, val)| {
                let ph = self.add(val);
                format!("{} = {}", quote_ident(col), ph)
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Build a WHERE clause, adding parameters as needed.
    fn add_where_clause(&mut self, where_clause: &WhereClause) -> String {
        match where_clause {
            WhereClause::Eq(col, val) => {
                let ph = self.add(val);
                format!("{} = {}", quote_ident(col), ph)
            }
            WhereClause::And(conditions) => conditions
                .iter()
                .map(|(col, val)| {
                    let ph = self.add(val);
                    format!("{} = {}", quote_ident(col), ph)
                })
                .collect::<Vec<_>>()
                .join(" AND "),
            WhereClause::Raw {
                condition,
                params: raw_params,
            } => {
                for p in raw_params {
                    self.params.push(convert_db_value(p));
                }
                condition.clone()
            }
        }
    }

    /// Consume the builder and return the collected parameters.
    fn finish(self) -> Vec<SqlParam> {
        self.params
    }
}

fn build_insert_sql(
    table: &str,
    columns: &[String],
    values: &[DbValue],
) -> (String, Vec<SqlParam>) {
    let mut builder = QueryBuilder::new();
    let cols = quote_cols(columns);
    let placeholders_str = builder.add_values(values, ", ");

    let sql = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        table, cols, placeholders_str
    );
    (sql, builder.finish())
}

fn build_upsert_sql(
    table: &str,
    columns: &[String],
    values: &[DbValue],
    conflict_columns: &[String],
    update_columns: &[String],
    update_condition: Option<&str>,
) -> (String, Vec<SqlParam>) {
    let mut builder = QueryBuilder::new();
    let cols = quote_cols(columns);
    let placeholders_str = builder.add_values(values, ", ");

    let conflict_cols = quote_cols(conflict_columns);
    let updates_str = update_columns
        .iter()
        .map(|c| format!("{} = EXCLUDED.{}", quote_ident(c), quote_ident(c)))
        .collect::<Vec<_>>()
        .join(", ");

    let sql = if update_columns.is_empty() {
        format!(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING",
            table, cols, placeholders_str, conflict_cols
        )
    } else if let Some(condition) = update_condition {
        format!(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {} WHERE {}",
            table, cols, placeholders_str, conflict_cols, updates_str, condition
        )
    } else {
        format!(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}",
            table, cols, placeholders_str, conflict_cols, updates_str
        )
    };

    (sql, builder.finish())
}

fn build_update_sql(
    table: &str,
    set_columns: &[(String, DbValue)],
    where_clause: &WhereClause,
) -> (String, Vec<SqlParam>) {
    let mut builder = QueryBuilder::new();
    let sets_str = builder.add_set_clause(set_columns);
    let where_str = builder.add_where_clause(where_clause);

    let sql = format!("UPDATE {} SET {} WHERE {}", table, sets_str, where_str);
    (sql, builder.finish())
}

fn build_delete_sql(table: &str, where_clause: &WhereClause) -> (String, Vec<SqlParam>) {
    let mut builder = QueryBuilder::new();
    let where_str = builder.add_where_clause(where_clause);

    let sql = format!("DELETE FROM {} WHERE {}", table, where_str);
    (sql, builder.finish())
}

// ─── Deterministic lock ordering ────────────────────────────────────────────

/// Sort operations by (table, row-identity bytes) so that every transaction
/// acquires row locks in the same deterministic order, preventing deadlocks
/// when concurrent transactions touch overlapping rows.
fn sort_operations_for_lock_ordering(ops: &mut [DbOperation]) {
    ops.sort_by_cached_key(operation_sort_key);
}

/// Extract a comparable sort key from a DbOperation.
///
/// For Upserts, the key is (table, conflict-column values) since those
/// identify the locked row. For Inserts the full value tuple is used.
/// For Update/Delete the WHERE clause values are used. RawSql sorts last.
fn operation_sort_key(op: &DbOperation) -> (u8, Vec<u8>) {
    match op {
        DbOperation::Upsert {
            table,
            columns,
            values,
            conflict_columns,
            ..
        } => {
            let mut key = table.as_bytes().to_vec();
            key.push(0); // separator
            for conflict_col in conflict_columns {
                if let Some(idx) = columns.iter().position(|c| c == conflict_col) {
                    if let Some(val) = values.get(idx) {
                        append_db_value_bytes(val, &mut key);
                    }
                }
            }
            (0, key)
        }
        DbOperation::Insert { table, values, .. } => {
            let mut key = table.as_bytes().to_vec();
            key.push(0);
            for val in values {
                append_db_value_bytes(val, &mut key);
            }
            (1, key)
        }
        DbOperation::Update {
            table,
            where_clause,
            ..
        } => {
            let mut key = table.as_bytes().to_vec();
            key.push(0);
            append_where_clause_bytes(where_clause, &mut key);
            (2, key)
        }
        DbOperation::Delete {
            table,
            where_clause,
        } => {
            let mut key = table.as_bytes().to_vec();
            key.push(0);
            append_where_clause_bytes(where_clause, &mut key);
            (3, key)
        }
        DbOperation::RawSql { query, .. } => (4, query.as_bytes().to_vec()),
        DbOperation::NamedSql { template, .. } => (5, template.as_bytes().to_vec()),
    }
}

/// Append a deterministic byte representation of a DbValue for sorting.
fn append_db_value_bytes(val: &DbValue, out: &mut Vec<u8>) {
    match val {
        DbValue::Null => out.push(0),
        DbValue::Bool(v) => out.push(if *v { 1 } else { 0 }),
        DbValue::Int64(v) => out.extend_from_slice(&v.to_be_bytes()),
        DbValue::Int32(v) => out.extend_from_slice(&v.to_be_bytes()),
        DbValue::Int2(v) => out.extend_from_slice(&v.to_be_bytes()),
        DbValue::Uint64(v) => out.extend_from_slice(&v.to_be_bytes()),
        DbValue::Float64(v) => out.extend_from_slice(&v.to_be_bytes()),
        DbValue::Text(v) | DbValue::VarChar(v) | DbValue::Numeric(v) => {
            out.extend_from_slice(v.as_bytes());
        }
        DbValue::Bytes(v) => out.extend_from_slice(v),
        DbValue::Address(v) => out.extend_from_slice(v),
        DbValue::Pubkey(v) => out.extend_from_slice(v),
        DbValue::Bytes32(v) => out.extend_from_slice(v),
        DbValue::Timestamp(v) => out.extend_from_slice(&v.to_be_bytes()),
        DbValue::Json(v) | DbValue::JsonB(v) => {
            out.extend_from_slice(v.to_string().as_bytes());
        }
    }
    out.push(0xFF); // separator between values
}

/// Append bytes from a WHERE clause for sort key generation.
fn append_where_clause_bytes(wc: &WhereClause, out: &mut Vec<u8>) {
    match wc {
        WhereClause::Eq(col, val) => {
            out.extend_from_slice(col.as_bytes());
            out.push(0);
            append_db_value_bytes(val, out);
        }
        WhereClause::And(pairs) => {
            for (col, val) in pairs {
                out.extend_from_slice(col.as_bytes());
                out.push(0);
                append_db_value_bytes(val, out);
            }
        }
        WhereClause::Raw { condition, params } => {
            out.extend_from_slice(condition.as_bytes());
            for p in params {
                append_db_value_bytes(p, out);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::LiveDbValue;

    /// DbValue::Float64 should convert to SqlParam::Float64 (not Timestamp).
    #[test]
    fn float64_converts_to_sql_param_float64() {
        let value = DbValue::Float64(3.14);
        let param = convert_db_value(&value);
        match param {
            SqlParam::Float64(v) => {
                assert!((v - 3.14).abs() < f64::EPSILON);
            }
            other => panic!("Expected SqlParam::Float64, got {:?}", other),
        }
    }

    /// DbValue::Float64 should use the default placeholder (no cast).
    #[test]
    fn float64_placeholder_is_plain() {
        let value = DbValue::Float64(1.0);
        let ph = placeholder_for(&value, 1);
        assert_eq!(ph, "$1");
    }

    /// DbValue::Int2 should convert to SqlParam::Int16 and preserve values
    /// outside the 0..255 range that the old u8 representation could not hold.
    #[test]
    fn int2_converts_to_sql_param_int16() {
        let value = DbValue::Int2(256);
        let param = convert_db_value(&value);
        match param {
            SqlParam::Int16(v) => assert_eq!(v, 256),
            other => panic!("Expected SqlParam::Int16(256), got {:?}", other),
        }
    }

    /// Negative i16 values should round-trip correctly through DbValue::Int2.
    #[test]
    fn int2_handles_negative_values() {
        let value = DbValue::Int2(-1);
        let param = convert_db_value(&value);
        match param {
            SqlParam::Int16(v) => assert_eq!(v, -1),
            other => panic!("Expected SqlParam::Int16(-1), got {:?}", other),
        }
    }

    /// DbValue::Int2 should use the default placeholder (no text cast needed).
    #[test]
    fn int2_placeholder_is_plain() {
        let value = DbValue::Int2(42);
        let ph = placeholder_for(&value, 3);
        assert_eq!(ph, "$3");
    }

    /// LiveDbValue::Float64 round-trips through DbValue::Float64 correctly.
    #[test]
    fn live_db_value_float64_roundtrip() {
        let original = DbValue::Float64(2.718);
        let live = LiveDbValue::from_db_value(&original);
        match &live {
            LiveDbValue::Float64(v) => assert!((v - 2.718).abs() < f64::EPSILON),
            other => panic!("Expected LiveDbValue::Float64, got {:?}", other),
        }
        let restored = live.to_db_value();
        match restored {
            DbValue::Float64(v) => assert!((v - 2.718).abs() < f64::EPSILON),
            other => panic!("Expected DbValue::Float64, got {:?}", other),
        }
    }

    /// LiveDbValue::Int2 round-trips through DbValue::Int2 correctly,
    /// including values that the old u8 representation would have truncated.
    #[test]
    fn live_db_value_int2_roundtrip() {
        let original = DbValue::Int2(300);
        let live = LiveDbValue::from_db_value(&original);
        match &live {
            LiveDbValue::Int2(v) => assert_eq!(*v, 300),
            other => panic!("Expected LiveDbValue::Int2(300), got {:?}", other),
        }
        let restored = live.to_db_value();
        match restored {
            DbValue::Int2(v) => assert_eq!(v, 300),
            other => panic!("Expected DbValue::Int2(300), got {:?}", other),
        }
    }

    /// LiveDbValue::Int2 round-trips negative values correctly.
    #[test]
    fn live_db_value_int2_negative_roundtrip() {
        let original = DbValue::Int2(-128);
        let live = LiveDbValue::from_db_value(&original);
        let restored = live.to_db_value();
        match restored {
            DbValue::Int2(v) => assert_eq!(v, -128),
            other => panic!("Expected DbValue::Int2(-128), got {:?}", other),
        }
    }

    // ─── Lock ordering tests ────────────────────────────────────────

    fn upsert_user(chain_id: i64, address: [u8; 20]) -> DbOperation {
        DbOperation::Upsert {
            table: "users".to_string(),
            columns: vec![
                "chain_id".to_string(),
                "address".to_string(),
                "first_seen".to_string(),
                "last_seen".to_string(),
            ],
            values: vec![
                DbValue::Int64(chain_id),
                DbValue::Address(address),
                DbValue::Timestamp(1000),
                DbValue::Timestamp(1000),
            ],
            conflict_columns: vec!["chain_id".to_string(), "address".to_string()],
            update_columns: vec!["last_seen".to_string()],
            update_condition: None,
        }
    }

    fn insert_transfer(block: i64, log_idx: i64) -> DbOperation {
        DbOperation::Insert {
            table: "transfers".to_string(),
            columns: vec!["block_height".to_string(), "log_position".to_string()],
            values: vec![DbValue::Int64(block), DbValue::Int64(log_idx)],
        }
    }

    #[test]
    fn sort_upserts_by_address_deterministically() {
        let addr_a = [0x00; 20];
        let addr_b = [0xFF; 20];

        // Reverse order: B before A
        let mut ops = vec![upsert_user(1, addr_b), upsert_user(1, addr_a)];

        sort_operations_for_lock_ordering(&mut ops);

        // After sort: A before B (0x00 < 0xFF)
        match &ops[0] {
            DbOperation::Upsert { values, .. } => match &values[1] {
                DbValue::Address(a) => assert_eq!(*a, addr_a),
                other => panic!("Expected Address, got {:?}", other),
            },
            other => panic!("Expected Upsert, got {:?}", other),
        }
        match &ops[1] {
            DbOperation::Upsert { values, .. } => match &values[1] {
                DbValue::Address(a) => assert_eq!(*a, addr_b),
                other => panic!("Expected Address, got {:?}", other),
            },
            other => panic!("Expected Upsert, got {:?}", other),
        }
    }

    #[test]
    fn sort_groups_by_table_then_key() {
        let addr = [0x01; 20];

        let mut ops = vec![
            insert_transfer(100, 0),
            upsert_user(1, addr),
            insert_transfer(100, 1),
        ];

        sort_operations_for_lock_ordering(&mut ops);

        // Upserts (type 0) sort before Inserts (type 1),
        // and within inserts, "transfers" sorts by values.
        assert!(matches!(&ops[0], DbOperation::Upsert { table, .. } if table == "users"));
        assert!(matches!(&ops[1], DbOperation::Insert { table, .. } if table == "transfers"));
        assert!(matches!(&ops[2], DbOperation::Insert { table, .. } if table == "transfers"));
    }

    #[test]
    fn sort_is_stable_for_identical_keys() {
        let addr = [0x42; 20];
        let mut ops = vec![upsert_user(1, addr), upsert_user(1, addr)];

        // Should not panic or reorder arbitrarily
        sort_operations_for_lock_ordering(&mut ops);
        assert_eq!(ops.len(), 2);
    }

    // ── Named-parameter parser tests ───────────────────────────────

    #[test]
    fn named_sql_basic_substitution() {
        let (sql, params) = build_named_sql("SELECT :foo", &[("foo".into(), DbValue::Int64(42))]);
        assert_eq!(sql, "SELECT $1");
        assert_eq!(params.len(), 1);
        assert!(matches!(params[0], SqlParam::Int64(42)));
    }

    #[test]
    fn named_sql_numeric_auto_cast() {
        let (sql, params) = build_named_sql(
            "INSERT INTO t VALUES (:amount)",
            &[("amount".into(), DbValue::Numeric("123.45".into()))],
        );
        assert_eq!(sql, "INSERT INTO t VALUES ($1::text::numeric)");
        assert_eq!(params.len(), 1);
        assert!(matches!(params[0], SqlParam::Text(ref s) if s == "123.45"));
    }

    #[test]
    fn named_sql_jsonb_auto_cast() {
        let (sql, params) = build_named_sql(
            "INSERT INTO t VALUES (:data)",
            &[("data".into(), DbValue::JsonB(serde_json::json!({"k": "v"})))],
        );
        assert_eq!(sql, "INSERT INTO t VALUES ($1::jsonb)");
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn named_sql_timestamp_auto_cast() {
        let (sql, _) = build_named_sql(
            "WHERE ts > :ts",
            &[("ts".into(), DbValue::Timestamp(1700000000))],
        );
        assert_eq!(sql, "WHERE ts > to_timestamp($1)");
    }

    #[test]
    fn named_sql_deduplication() {
        let (sql, params) = build_named_sql(
            "WHERE chain_id = :cid AND s.chain_id = :cid",
            &[("cid".into(), DbValue::Int64(1))],
        );
        assert_eq!(sql, "WHERE chain_id = $1 AND s.chain_id = $1");
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn named_sql_cast_operator_preserved() {
        let (sql, _) = build_named_sql("SELECT 0::numeric, NULL::text", &[]);
        assert_eq!(sql, "SELECT 0::numeric, NULL::text");
    }

    #[test]
    fn named_sql_single_quoted_string_ignored() {
        let (sql, params) = build_named_sql("SELECT ':not_a_param'", &[]);
        assert_eq!(sql, "SELECT ':not_a_param'");
        assert!(params.is_empty());
    }

    #[test]
    fn named_sql_double_quoted_ident_ignored() {
        let (sql, params) = build_named_sql("SELECT \":not_a_param\"", &[]);
        assert_eq!(sql, "SELECT \":not_a_param\"");
        assert!(params.is_empty());
    }

    #[test]
    fn named_sql_escaped_single_quote() {
        let (sql, params) = build_named_sql("SELECT 'it''s :not_a_param'", &[]);
        assert_eq!(sql, "SELECT 'it''s :not_a_param'");
        assert!(params.is_empty());
    }

    #[test]
    fn named_sql_multiple_distinct_params() {
        let (sql, params) = build_named_sql(
            "WHERE a = :x AND b = :y",
            &[
                ("x".into(), DbValue::Int64(1)),
                ("y".into(), DbValue::Int32(2)),
            ],
        );
        assert_eq!(sql, "WHERE a = $1 AND b = $2");
        assert_eq!(params.len(), 2);
        assert!(matches!(params[0], SqlParam::Int64(1)));
        assert!(matches!(params[1], SqlParam::Int32(2)));
    }

    #[test]
    fn named_sql_null_no_cast() {
        let (sql, params) = build_named_sql("SELECT :val", &[("val".into(), DbValue::Null)]);
        assert_eq!(sql, "SELECT $1");
        assert_eq!(params.len(), 1);
        assert!(matches!(params[0], SqlParam::Null));
    }

    #[test]
    fn named_sql_param_followed_by_cast() {
        // :tick::integer — parser captures :tick, then ::integer is literal
        let (sql, _) = build_named_sql(
            "SELECT :tick::integer",
            &[("tick".into(), DbValue::Int32(100))],
        );
        assert_eq!(sql, "SELECT $1::integer");
    }

    #[test]
    #[should_panic(expected = "unbound parameter ':unknown'")]
    fn named_sql_missing_param_panics() {
        build_named_sql("SELECT :unknown", &[]);
    }

    #[test]
    fn named_query_builder_basic() {
        let chain_id: i64 = 8453;
        let block: i64 = 100;
        let (sql, params) =
            NamedQueryBuilder::new("WHERE chain_id = :cid AND block = :block AND chain_id = :cid")
                .bind("cid", &chain_id)
                .bind("block", &block)
                .build();

        assert_eq!(sql, "WHERE chain_id = $1 AND block = $2 AND chain_id = $1");
        assert_eq!(params.len(), 2);
    }
}
