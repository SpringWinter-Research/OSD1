//! Shared SQL request types. The lead owns this crate.
//!
//! The HTTP envelope in `spike/FINDINGS.md` is the contract the Cloudflare
//! `cloudflare@4.5.0` SDK accepted. JSON encoding of that envelope lives in
//! `osd1-http`. This crate is the value types both sides share.

use serde_json::Value;

/// SQL execution failed. The HTTP layer maps this to status 400, code [`ERROR_SQL`].
pub const ERROR_SQL: i64 = 7500;
/// The JSON body or a parameter type was rejected.
pub const ERROR_BAD_REQUEST: i64 = 7400;
/// Bearer token missing or wrong.
pub const ERROR_UNAUTHORIZED: i64 = 10000;
/// The bounded FIFO queue refused the request.
pub const ERROR_QUEUE_FULL: i64 = 10001;

/// Region literal accepted by the `cloudflare@4.5.0` `served_by_region` union.
pub const SERVED_BY_REGION: &str = "WNAM";

#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
}

impl SqlValue {
    pub fn to_json(&self) -> Value {
        match self {
            SqlValue::Null => Value::Null,
            SqlValue::Integer(value) => Value::from(*value),
            SqlValue::Real(value) => serde_json::Number::from_f64(*value)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            SqlValue::Text(value) => Value::String(value.clone()),
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ParamError {
    #[error("params must be an array")]
    NotArray,
    #[error("parameter {index} must be null, a string, an integer, or a finite number")]
    BadType { index: usize },
}

/// `None` and a missing field both mean no parameters.
/// JSON null, string, integer, and finite float are accepted.
/// Booleans, objects, and arrays are rejected.
pub fn parse_params(params: Option<Value>) -> Result<Vec<SqlValue>, ParamError> {
    let Some(params) = params else {
        return Ok(Vec::new());
    };
    let Value::Array(items) = params else {
        return Err(ParamError::NotArray);
    };
    items
        .into_iter()
        .enumerate()
        .map(|(index, value)| value_from_json(index, value))
        .collect()
}

fn value_from_json(index: usize, value: Value) -> Result<SqlValue, ParamError> {
    match value {
        Value::Null => Ok(SqlValue::Null),
        Value::String(text) => Ok(SqlValue::Text(text)),
        Value::Number(number) => {
            if let Some(integer) = number.as_i64() {
                Ok(SqlValue::Integer(integer))
            } else if let Some(real) = number.as_f64().filter(|value| value.is_finite()) {
                Ok(SqlValue::Real(real))
            } else {
                Err(ParamError::BadType { index })
            }
        }
        Value::Bool(_) | Value::Array(_) | Value::Object(_) => Err(ParamError::BadType { index }),
    }
}

/// Build a `/query` row object. Extra values are ignored. Missing values become null.
pub fn row_to_object(columns: &[String], values: &[SqlValue]) -> Value {
    let mut object = serde_json::Map::new();
    for (index, column) in columns.iter().enumerate() {
        let value = values.get(index).map(SqlValue::to_json).unwrap_or(Value::Null);
        object.insert(column.clone(), value);
    }
    Value::Object(object)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultShape {
    Objects,
    Raw,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SqlRequest {
    pub sql: String,
    pub params: Vec<SqlValue>,
    pub shape: ResultShape,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StatementMeta {
    pub changed_db: bool,
    pub changes: u64,
    pub duration_ms: f64,
    pub last_row_id: i64,
    pub rows_read: u64,
    pub rows_written: u64,
    pub size_after: u64,
    pub served_by_colo: String,
    pub served_by_region: String,
    pub served_by_primary: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StatementResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<SqlValue>>,
    pub meta: StatementMeta,
    pub success: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SqlResponse {
    pub statements: Vec<StatementResult>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SqlError {
    #[error("sqlite error {code}: {message}")]
    Sqlite { code: i32, message: String },
    #[error("{message}")]
    AuthorizerDenied { message: String },
    #[error("transaction was still open after the request")]
    TransactionLeftOpen,
    #[error("sql queue is full")]
    QueueFull,
    #[error("sql worker is shut down")]
    ShuttingDown,
}

#[derive(Debug)]
pub enum EnqueueResult {
    Accepted(tokio::sync::oneshot::Receiver<Result<SqlResponse, SqlError>>),
    Full,
    Closed,
}

pub trait SqlExecutor: Send + Sync {
    fn enqueue(&self, request: SqlRequest) -> EnqueueResult;
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_sdk_and_native_json_values() {
        let parsed = parse_params(Some(json!(["a", null, 2, -3, 1.5]))).unwrap();
        assert_eq!(
            parsed,
            vec![
                SqlValue::Text("a".to_string()),
                SqlValue::Null,
                SqlValue::Integer(2),
                SqlValue::Integer(-3),
                SqlValue::Real(1.5),
            ]
        );
    }

    #[test]
    fn missing_params_are_an_empty_list() {
        assert_eq!(parse_params(None).unwrap(), Vec::new());
    }

    #[test]
    fn rejects_bools_objects_and_non_arrays() {
        assert_eq!(
            parse_params(Some(json!([true]))),
            Err(ParamError::BadType { index: 0 })
        );
        assert_eq!(
            parse_params(Some(json!([{"n": 1}]))),
            Err(ParamError::BadType { index: 0 })
        );
        assert_eq!(parse_params(Some(json!("a"))), Err(ParamError::NotArray));
    }

    #[test]
    fn row_object_uses_column_names() {
        let row = row_to_object(
            &["n".to_string(), "missing".to_string()],
            &[SqlValue::Integer(1)],
        );
        assert_eq!(row, json!({"n": 1, "missing": null}));
    }

    #[test]
    fn error_codes_match_the_sdk_spike() {
        assert_eq!(ERROR_SQL, 7500);
        assert_eq!(ERROR_BAD_REQUEST, 7400);
        assert_eq!(ERROR_UNAUTHORIZED, 10000);
        assert_eq!(ERROR_QUEUE_FULL, 10001);
        assert_eq!(SERVED_BY_REGION, "WNAM");
    }
}
