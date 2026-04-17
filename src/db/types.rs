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
    /// Signed 16-bit integer (stored as SMALLINT/INT2)
    Int2(i16),
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
    /// Solana pubkey (32 bytes, stored as BYTEA)
    Pubkey([u8; 32]),
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
    /// Double-precision floating point (FLOAT8)
    Float64(f64),
}

impl DbValue {
    /// Check if the value is null
    #[allow(dead_code)]
    pub fn is_null(&self) -> bool {
        matches!(self, DbValue::Null)
    }

    /// Create a JSONB value from any serializable type
    pub fn jsonb<T: Serialize>(value: T) -> Self {
        DbValue::JsonB(serde_json::to_value(value).expect("Failed to serialize to JSON"))
    }

    /// Create a JSON value from any serializable type
    #[allow(dead_code)]
    pub fn json<T: Serialize>(value: T) -> Self {
        DbValue::Json(serde_json::to_value(value).expect("Failed to serialize to JSON"))
    }
}

/// Database operation returned by handlers.
#[allow(dead_code)]
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
        /// Optional WHERE clause on the DO UPDATE (e.g. "EXCLUDED.block_height >= pool_state.block_height")
        update_condition: Option<String>,
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
    /// Raw SQL for complex operations (use sparingly).
    ///
    /// `snapshot` is optional metadata for live-mode rollback. When present,
    /// the executor snapshots the targeted row before executing the SQL and can
    /// later restore it during reorg handling.
    RawSql {
        query: String,
        params: Vec<DbValue>,
        snapshot: Option<DbSnapshot>,
    },
    /// Named-parameter SQL for complex operations.
    ///
    /// Like `RawSql`, but uses `:param_name` placeholders instead of positional
    /// `$N`. The build step replaces each `:name` with the correct `$N` and
    /// applies type-specific casts via `placeholder_for()` (e.g.
    /// `$5::text::numeric` for `DbValue::Numeric`), eliminating manual cast
    /// errors.
    ///
    /// Same `:name` referenced multiple times reuses the same `$N` index.
    /// Template names not found in params cause a panic with a descriptive
    /// message.
    NamedSql {
        template: String,
        params: Vec<(String, DbValue)>,
        snapshot: Option<DbSnapshot>,
    },
}

/// Snapshot metadata for row-modifying operations that cannot be expressed as a
/// standard Upsert.
#[derive(Debug, Clone)]
pub struct DbSnapshot {
    pub table: String,
    pub key_columns: Vec<(String, DbValue)>,
}

/// WHERE clause for UPDATE and DELETE operations.
#[derive(Debug, Clone)]
#[allow(dead_code)]
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
