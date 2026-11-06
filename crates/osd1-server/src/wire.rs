//! JSON stored in the raft log for one applied SQL command.
//!
//! The HTTP layer still builds the D1 envelope from [`SqlResponse`]. This
//! format only carries that value from the apply hook back to the client.

use osd1_types::{
    parse_params, ResultShape, SqlError, SqlRequest, SqlResponse, SqlValue, StatementMeta,
    StatementResult,
};
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::sql_error;

#[derive(Serialize, Deserialize)]
struct WireBody {
    kind: String,
    #[serde(default)]
    message: String,
    #[serde(default)]
    code: i32,
    #[serde(default)]
    statements: Vec<WireStatement>,
}

#[derive(Serialize, Deserialize)]
struct WireStatement {
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
    success: bool,
    meta: WireMeta,
}

#[derive(Serialize, Deserialize)]
struct WireMeta {
    changed_db: bool,
    changes: u64,
    duration_ms: f64,
    last_row_id: i64,
    rows_read: u64,
    rows_written: u64,
    size_after: u64,
    served_by_colo: String,
    served_by_region: String,
    served_by_primary: bool,
}

pub(crate) fn params_json(params: &[SqlValue]) -> Result<String, SqlError> {
    let values: Vec<Value> = params.iter().map(SqlValue::to_json).collect();
    serde_json::to_string(&values).map_err(|error| sql_error(error.to_string()))
}

pub(crate) fn command_request(sql: &str, params_json: &str) -> Result<SqlRequest, SqlError> {
    let value = serde_json::from_str(params_json).map_err(|error| sql_error(error.to_string()))?;
    let params = parse_params(Some(value)).map_err(|error| sql_error(error.to_string()))?;
    Ok(SqlRequest {
        sql: sql.to_string(),
        params,
        shape: ResultShape::Objects,
    })
}

pub(crate) fn encode_result(result: Result<SqlResponse, SqlError>) -> String {
    let body = match result {
        Ok(response) => WireBody {
            kind: "ok".to_string(),
            message: String::new(),
            code: 0,
            statements: response
                .statements
                .into_iter()
                .map(wire_statement)
                .collect(),
        },
        Err(SqlError::AuthorizerDenied { message }) => WireBody {
            kind: "denied".to_string(),
            message,
            code: 0,
            statements: Vec::new(),
        },
        Err(SqlError::Sqlite { code, message }) => WireBody {
            kind: "sqlite".to_string(),
            message,
            code,
            statements: Vec::new(),
        },
        Err(other) => WireBody {
            kind: "sqlite".to_string(),
            message: other.to_string(),
            code: 1,
            statements: Vec::new(),
        },
    };
    serde_json::to_string(&body).unwrap_or_else(|_| {
        r#"{"kind":"sqlite","code":1,"message":"could not encode the sql result"}"#.to_string()
    })
}

pub(crate) fn decode_body(body: &str) -> Result<SqlResponse, SqlError> {
    if body.is_empty() {
        return Err(sql_error("committed command has no result yet".to_string()));
    }
    let parsed: WireBody = serde_json::from_str(body)
        .map_err(|error| sql_error(format!("bad raft result: {error}")))?;
    match parsed.kind.as_str() {
        "ok" => {
            let mut statements = Vec::with_capacity(parsed.statements.len());
            for statement in parsed.statements {
                statements.push(statement_from_wire(statement)?);
            }
            Ok(SqlResponse { statements })
        }
        "denied" => Err(SqlError::AuthorizerDenied {
            message: parsed.message,
        }),
        "sqlite" => Err(SqlError::Sqlite {
            code: parsed.code,
            message: parsed.message,
        }),
        other => Err(sql_error(format!("bad raft result kind {other}"))),
    }
}

fn wire_statement(statement: StatementResult) -> WireStatement {
    WireStatement {
        columns: statement.columns.iter().cloned().collect(),
        rows: statement
            .rows
            .iter()
            .map(|row| row.iter().map(SqlValue::to_json).collect())
            .collect(),
        success: statement.success,
        meta: WireMeta {
            changed_db: statement.meta.changed_db,
            changes: statement.meta.changes,
            duration_ms: statement.meta.duration_ms,
            last_row_id: statement.meta.last_row_id,
            rows_read: statement.meta.rows_read,
            rows_written: statement.meta.rows_written,
            size_after: statement.meta.size_after,
            served_by_colo: statement.meta.served_by_colo.as_ref().to_string(),
            served_by_region: statement.meta.served_by_region.as_ref().to_string(),
            served_by_primary: statement.meta.served_by_primary,
        },
    }
}

fn statement_from_wire(statement: WireStatement) -> Result<StatementResult, SqlError> {
    let mut rows = Vec::with_capacity(statement.rows.len());
    for row in statement.rows {
        let mut values = Vec::with_capacity(row.len());
        for value in row {
            values.push(cell(value)?);
        }
        rows.push(values);
    }
    Ok(StatementResult {
        columns: std::sync::Arc::<[String]>::from(statement.columns),
        rows,
        success: statement.success,
        meta: StatementMeta {
            changed_db: statement.meta.changed_db,
            changes: statement.meta.changes,
            duration_ms: statement.meta.duration_ms,
            last_row_id: statement.meta.last_row_id,
            rows_read: statement.meta.rows_read,
            rows_written: statement.meta.rows_written,
            size_after: statement.meta.size_after,
            served_by_colo: std::sync::Arc::from(statement.meta.served_by_colo),
            served_by_region: std::sync::Arc::from(statement.meta.served_by_region),
            served_by_primary: statement.meta.served_by_primary,
        },
    })
}

fn cell(value: Value) -> Result<SqlValue, SqlError> {
    let mut parsed = parse_params(Some(Value::Array(vec![value])))
        .map_err(|error| sql_error(error.to_string()))?;
    parsed
        .pop()
        .ok_or_else(|| sql_error("empty raft value".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn sql_result_round_trips_through_the_log_body() {
        let response = SqlResponse {
            statements: vec![StatementResult {
                columns: Arc::from(vec!["n".to_string()]),
                rows: vec![vec![SqlValue::Integer(7), SqlValue::Text("a".into())]],
                success: true,
                meta: StatementMeta {
                    changed_db: true,
                    changes: 1,
                    duration_ms: 0.5,
                    last_row_id: 7,
                    rows_read: 0,
                    rows_written: 1,
                    size_after: 12,
                    served_by_colo: Arc::from("LOC"),
                    served_by_region: Arc::from("WNAM"),
                    served_by_primary: true,
                },
            }],
        };
        let encoded = encode_result(Ok(response.clone()));
        let decoded = decode_body(&encoded).unwrap();
        assert_eq!(decoded, response);
    }

    #[test]
    fn an_authorizer_denial_round_trips() {
        let encoded = encode_result(Err(SqlError::AuthorizerDenied {
            message: "nondeterministic function is not allowed".into(),
        }));
        match decode_body(&encoded) {
            Err(SqlError::AuthorizerDenied { message }) => {
                assert!(message.contains("nondeterministic"));
            }
            other => panic!("expected a denial, got {other:?}"),
        }
    }

    #[test]
    fn an_empty_body_is_not_applied_again() {
        match decode_body("") {
            Err(SqlError::Sqlite { message, .. }) => {
                assert!(message.contains("no result"));
            }
            other => panic!("expected an empty-body error, got {other:?}"),
        }
    }
}
