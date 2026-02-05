use bytes::BytesMut;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod, Runtime};
use tokio_postgres::types::ToSql;
use tokio_postgres::NoTls;

use super::error::DbError;
use super::types::{DbOperation, DbValue, WhereClause};

pub struct DbPool {
    pool: Pool,
}

impl DbPool {
    pub async fn new(database_url: &str) -> Result<Self, DbError> {
        let config = database_url
            .parse::<tokio_postgres::Config>()
            .map_err(|e| DbError::InvalidConnectionString(e.to_string()))?;

        let manager_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };

        let manager = Manager::from_config(config, NoTls, manager_config);

        let pool = Pool::builder(manager)
            .max_size(16)
            .runtime(Runtime::Tokio1)
            .build()
            .map_err(DbError::BuildError)?;

        let _conn = pool.get().await?;
        tracing::info!("Database connection pool created successfully");

        Ok(Self { pool })
    }

    pub fn inner(&self) -> &Pool {
        &self.pool
    }

    pub async fn execute_transaction(&self, operations: Vec<DbOperation>) -> Result<(), DbError> {
        if operations.is_empty() {
            return Ok(());
        }

        let mut client = self.pool.get().await?;
        let transaction = client.transaction().await?;

        for op in operations {
            match op {
                DbOperation::Upsert {
                    table,
                    columns,
                    values,
                    conflict_columns,
                    update_columns,
                } => {
                    let (sql, params) =
                        build_upsert_sql(&table, &columns, &values, &conflict_columns, &update_columns);
                    let params_refs: Vec<&(dyn ToSql + Sync)> =
                        params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
                    if let Err(e) = transaction.execute(&sql, &params_refs[..]).await {
                        tracing::error!("SQL execution failed: {}\nSQL: {}\nError: {:?}", e, sql, e);
                        return Err(e.into());
                    }
                }
                DbOperation::Insert {
                    table,
                    columns,
                    values,
                } => {
                    let (sql, params) = build_insert_sql(&table, &columns, &values);
                    let params_refs: Vec<&(dyn ToSql + Sync)> =
                        params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
                    transaction.execute(&sql, &params_refs[..]).await?;
                }
                DbOperation::Update {
                    table,
                    set_columns,
                    where_clause,
                } => {
                    let (sql, params) = build_update_sql(&table, &set_columns, &where_clause);
                    let params_refs: Vec<&(dyn ToSql + Sync)> =
                        params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
                    transaction.execute(&sql, &params_refs[..]).await?;
                }
                DbOperation::Delete { table, where_clause } => {
                    let (sql, params) = build_delete_sql(&table, &where_clause);
                    let params_refs: Vec<&(dyn ToSql + Sync)> =
                        params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
                    transaction.execute(&sql, &params_refs[..]).await?;
                }
                DbOperation::RawSql { query, params } => {
                    let converted = convert_values_to_params(&params);
                    let params_refs: Vec<&(dyn ToSql + Sync)> =
                        converted.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
                    transaction.execute(&query, &params_refs[..]).await?;
                }
            }
        }

        transaction.commit().await?;
        Ok(())
    }

    pub async fn run_migrations(&self) -> Result<(), DbError> {
        super::migrations::run(&self.pool).await
    }

    pub async fn query(
        &self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<tokio_postgres::Row>, DbError> {
        let client = self.pool.get().await?;
        let rows = client.query(query, params).await?;
        Ok(rows)
    }
}

#[derive(Debug)]
enum SqlParam {
    Null,
    Bool(bool),
    Int64(i64),
    Int32(i32),
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
            SqlParam::Text(v) => v.to_sql(ty, out),
            SqlParam::Bytes(v) => v.to_sql(ty, out),
            SqlParam::Json(v) => v.to_sql(ty, out),
        }
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        <bool as ToSql>::accepts(ty)
            || <i64 as ToSql>::accepts(ty)
            || <i32 as ToSql>::accepts(ty)
            || <String as ToSql>::accepts(ty)
            || <Vec<u8> as ToSql>::accepts(ty)
            || <serde_json::Value as ToSql>::accepts(ty)
    }

    tokio_postgres::types::to_sql_checked!();
}

fn convert_db_value(value: &DbValue) -> SqlParam {
    match value {
        DbValue::Null => SqlParam::Null,
        DbValue::Bool(v) => SqlParam::Bool(*v),
        DbValue::Int64(v) => SqlParam::Int64(*v),
        DbValue::Int32(v) => SqlParam::Int32(*v),
        DbValue::Uint64(v) => SqlParam::Int64(*v as i64),
        DbValue::Text(v) => SqlParam::Text(v.clone()),
        DbValue::Bytes(v) => SqlParam::Bytes(v.clone()),
        DbValue::Address(v) => SqlParam::Bytes(v.to_vec()),
        DbValue::Bytes32(v) => SqlParam::Bytes(v.to_vec()),
        DbValue::Numeric(v) => SqlParam::Text(v.clone()), // NUMERIC accepts text
        DbValue::Timestamp(v) => SqlParam::Int64(*v),
        DbValue::Json(v) => SqlParam::Json(v.clone()),
    }
}

fn convert_values_to_params(values: &[DbValue]) -> Vec<SqlParam> {
    values.iter().map(convert_db_value).collect()
}

fn build_insert_sql(table: &str, columns: &[String], values: &[DbValue]) -> (String, Vec<SqlParam>) {
    let cols = columns.join(", ");
    let placeholders: Vec<String> = (1..=values.len()).map(|i| format!("${}", i)).collect();
    let placeholders_str = placeholders.join(", ");

    let sql = format!("INSERT INTO {} ({}) VALUES ({})", table, cols, placeholders_str);
    let params = convert_values_to_params(values);

    (sql, params)
}

fn build_upsert_sql(
    table: &str,
    columns: &[String],
    values: &[DbValue],
    conflict_columns: &[String],
    update_columns: &[String],
) -> (String, Vec<SqlParam>) {
    let cols = columns.join(", ");
    let placeholders: Vec<String> = (1..=values.len()).map(|i| format!("${}", i)).collect();
    let placeholders_str = placeholders.join(", ");

    let conflict_cols = conflict_columns.join(", ");
    let updates: Vec<String> = update_columns
        .iter()
        .map(|c| format!("{} = EXCLUDED.{}", c, c))
        .collect();
    let updates_str = updates.join(", ");

    let sql = if update_columns.is_empty() {
        format!(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING",
            table, cols, placeholders_str, conflict_cols
        )
    } else {
        format!(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}",
            table, cols, placeholders_str, conflict_cols, updates_str
        )
    };

    let params = convert_values_to_params(values);
    (sql, params)
}

fn build_update_sql(
    table: &str,
    set_columns: &[(String, DbValue)],
    where_clause: &WhereClause,
) -> (String, Vec<SqlParam>) {
    let mut params = Vec::new();
    let mut param_idx = 1;

    let sets: Vec<String> = set_columns
        .iter()
        .map(|(col, val)| {
            params.push(convert_db_value(val));
            let s = format!("{} = ${}", col, param_idx);
            param_idx += 1;
            s
        })
        .collect();
    let sets_str = sets.join(", ");

    let where_str = match where_clause {
        WhereClause::Eq(col, val) => {
            params.push(convert_db_value(val));
            let s = format!("{} = ${}", col, param_idx);
            s
        }
        WhereClause::And(conditions) => {
            let parts: Vec<String> = conditions
                .iter()
                .map(|(col, val)| {
                    params.push(convert_db_value(val));
                    let s = format!("{} = ${}", col, param_idx);
                    param_idx += 1;
                    s
                })
                .collect();
            parts.join(" AND ")
        }
        WhereClause::Raw { condition, params: raw_params } => {
            for p in raw_params {
                params.push(convert_db_value(p));
            }
            condition.clone()
        }
    };

    let sql = format!("UPDATE {} SET {} WHERE {}", table, sets_str, where_str);
    (sql, params)
}

fn build_delete_sql(table: &str, where_clause: &WhereClause) -> (String, Vec<SqlParam>) {
    let mut params = Vec::new();
    let mut param_idx = 1;

    let where_str = match where_clause {
        WhereClause::Eq(col, val) => {
            params.push(convert_db_value(val));
            format!("{} = ${}", col, param_idx)
        }
        WhereClause::And(conditions) => {
            let parts: Vec<String> = conditions
                .iter()
                .map(|(col, val)| {
                    params.push(convert_db_value(val));
                    let s = format!("{} = ${}", col, param_idx);
                    param_idx += 1;
                    s
                })
                .collect();
            parts.join(" AND ")
        }
        WhereClause::Raw { condition, params: raw_params } => {
            for p in raw_params {
                params.push(convert_db_value(p));
            }
            condition.clone()
        }
    };

    let sql = format!("DELETE FROM {} WHERE {}", table, where_str);
    (sql, params)
}
