use serde::Serialize;
use serde_json::Value as JsonValue;

/// A value that can be stored in the database.
#[derive(Debug, Clone)]
pub enum DbValue {
    /// NULL value
    Null,
    /// Boolean
    Bool(bool),
    /// Signed 64-bit integer
    Int64(i64),
    /// Signed 32-bit integer
    Int32(i32),
    /// Unsigned 8-bit integer (stored as SMALLINT/INT2)
    Int2(u8),
    /// Unsigned 64-bit integer (stored as BIGINT)
    Uint64(u64),
    /// Text (unlimited length)
    Text(String),
    /// VARCHAR (length constraint enforced at schema level)
    VarChar(String),
    /// Raw bytes (stored as BYTEA)
    Bytes(Vec<u8>),
    /// Ethereum address (20 bytes, stored as BYTEA)
    Address([u8; 20]),
    /// 32-byte hash (stored as BYTEA)
    Bytes32([u8; 32]),
    /// Numeric string for uint256/int256 (stored as NUMERIC)
    Numeric(String),
    /// Unix timestamp (stored as BIGINT)
    Timestamp(i64),
    /// JSON value
    Json(JsonValue),
    /// JSONB value (binary JSON, more efficient for querying)
    JsonB(JsonValue),
}

impl DbValue {
    /// Check if the value is null
    pub fn is_null(&self) -> bool {
        matches!(self, DbValue::Null)
    }

    /// Create a JSONB value from any serializable type
    pub fn jsonb<T: Serialize>(value: T) -> Self {
        DbValue::JsonB(serde_json::to_value(value).expect("Failed to serialize to JSON"))
    }

    /// Create a JSON value from any serializable type
    pub fn json<T: Serialize>(value: T) -> Self {
        DbValue::Json(serde_json::to_value(value).expect("Failed to serialize to JSON"))
    }
}

/// Database operation returned by handlers.
#[derive(Debug, Clone)]
pub enum DbOperation {
    /// INSERT with ON CONFLICT DO UPDATE (upsert)
    Upsert {
        table: String,
        columns: Vec<String>,
        values: Vec<DbValue>,
        /// Columns that form the unique constraint
        conflict_columns: Vec<String>,
        /// Columns to update on conflict
        update_columns: Vec<String>,
    },
    /// Simple INSERT
    Insert {
        table: String,
        columns: Vec<String>,
        values: Vec<DbValue>,
    },
    /// UPDATE with WHERE clause
    Update {
        table: String,
        set_columns: Vec<(String, DbValue)>,
        where_clause: WhereClause,
    },
    /// DELETE with WHERE clause
    Delete {
        table: String,
        where_clause: WhereClause,
    },
    /// Raw SQL for complex operations (use sparingly)
    RawSql {
        query: String,
        params: Vec<DbValue>,
    },
}

/// WHERE clause for UPDATE and DELETE operations.
#[derive(Debug, Clone)]
pub enum WhereClause {
    /// column = value
    Eq(String, DbValue),
    /// column1 = value1 AND column2 = value2 AND ...
    And(Vec<(String, DbValue)>),
    /// Raw SQL condition with parameters
    Raw {
        condition: String,
        params: Vec<DbValue>,
    },
}
