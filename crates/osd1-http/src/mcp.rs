//! `POST /mcp`. Tools run on the same SQL queue as `/query`.
//!
//! Legacy clients use `initialize`, then `tools/list` and `tools/call`
//! (`2025-03-26`, `2025-06-18`, `2025-11-25`). A `2026-07-28` client sends
//! `_meta`, `MCP-Protocol-Version`, and `Mcp-Method` on every request, and
//! `Mcp-Name` on `tools/call`. This process does not keep a session and does
//! not stream events. A pure router does not enqueue.

use std::sync::atomic::Ordering;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use osd1_types::{parse_params, ResultShape, SqlRequest, SqlValue};
use serde_json::{json, Value};

use crate::{authorize, execute_local, json_response, leaving, AppState};

const META_VERSION: &str = "io.modelcontextprotocol/protocolVersion";
const META_SERVER: &str = "io.modelcontextprotocol/serverInfo";
const V_MODERN: &str = "2026-07-28";
const V_NOV: &str = "2025-11-25";
const V_JUN: &str = "2025-06-18";
const V_MAR: &str = "2025-03-26";
const SERVER_NAME: &str = "osd1";
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");
const TTL_MS: u64 = 3_600_000;
const LIST_TABLES_SQL: &str = "SELECT name, type FROM sqlite_master WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%' AND name != 'osd1_applied' ORDER BY name";
const DESCRIBE_TABLE_SQL: &str = "SELECT type, name, sql FROM sqlite_master WHERE name = ?1 AND type IN ('table', 'view');\nSELECT cid, name, type, \"notnull\", dflt_value, pk FROM pragma_table_info(?1) ORDER BY cid;\nSELECT name, \"unique\", origin, partial FROM pragma_index_list(?1) ORDER BY seq";
const INSTRUCTIONS: &str = "OSD1 is one SQLite database. query runs SQL. create_table, alter_table, and drop_table build those statements. list_tables reads sqlite_master. describe_table reads one table's columns, indexes, and CREATE text. load_extension stays denied. Call this endpoint on the process that owns the database.";

pub(crate) async fn method_not_allowed() -> Response {
    (
        StatusCode::METHOD_NOT_ALLOWED,
        [(header::ALLOW, HeaderValue::from_static("POST"))],
    )
        .into_response()
}

pub(crate) async fn post(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !content_type_is_json(&headers) {
        return StatusCode::UNSUPPORTED_MEDIA_TYPE.into_response();
    }
    if !accepts_json(&headers) {
        return StatusCode::NOT_ACCEPTABLE.into_response();
    }
    if state.draining.load(Ordering::Relaxed) {
        return leaving();
    }
    if let Err(response) = authorize(&state, &headers) {
        return *response;
    }
    let value: Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => return rpc_error(StatusCode::OK, Value::Null, -32700, "parse error", None),
    };
    if value.is_array() {
        return rpc_error(StatusCode::OK, Value::Null, -32600, "invalid request", None);
    }
    let Some(message) = value.as_object() else {
        return rpc_error(StatusCode::OK, Value::Null, -32600, "invalid request", None);
    };
    let id = message.get("id").cloned().unwrap_or(Value::Null);
    if message.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return rpc_error(StatusCode::OK, id, -32600, "invalid request", None);
    }
    if message.contains_key("id") && !valid_id(&id) {
        return rpc_error(StatusCode::OK, Value::Null, -32600, "invalid request", None);
    }
    let Some(method) = message.get("method").and_then(Value::as_str) else {
        return rpc_error(StatusCode::OK, id, -32600, "invalid request", None);
    };
    if method.is_empty() {
        return rpc_error(StatusCode::OK, id, -32600, "invalid request", None);
    }
    if !message.contains_key("id") {
        return StatusCode::ACCEPTED.into_response();
    }
    let params = message.get("params").cloned().unwrap_or(Value::Null);
    let meta_version = match meta_version(message) {
        Ok(version) => version,
        Err(()) => return header_mismatch(id),
    };
    let header_version = header_text(&headers, "mcp-protocol-version");
    if method != "initialize" {
        if let (Some(header), Some(meta)) = (header_version.as_deref(), meta_version.as_deref()) {
            if header != meta {
                return header_mismatch(id);
            }
        }
        if let Some(version) = meta_version.as_deref().or(header_version.as_deref()) {
            if !known_version(version) {
                return unsupported(id, version);
            }
            if version == V_MODERN
                && (header_version.as_deref() != Some(V_MODERN)
                    || meta_version.as_deref() != Some(V_MODERN))
            {
                return header_mismatch(id);
            }
        }
    }
    let modern = method != "initialize"
        && meta_version.as_deref() == Some(V_MODERN)
        && header_version.as_deref() == Some(V_MODERN);
    if modern {
        if header_text(&headers, "mcp-method").as_deref() != Some(method) {
            return header_mismatch(id);
        }
    }
    dispatch(&state, &headers, id, method, &params, modern).await
}

async fn dispatch(
    state: &AppState,
    headers: &HeaderMap,
    id: Value,
    method: &str,
    params: &Value,
    modern: bool,
) -> Response {
    match method {
        "initialize" => initialize(id, params),
        "ping" => rpc_ok(id, json!({})),
        "server/discover" => discover(id),
        "tools/list" => tools_list(id, params, modern),
        "tools/call" => call_tool(state, headers, id, params, modern).await,
        _ if modern => rpc_error(StatusCode::NOT_FOUND, id, -32601, "method not found", None),
        _ => rpc_error(StatusCode::OK, id, -32601, "method not found", None),
    }
}

fn initialize(id: Value, params: &Value) -> Response {
    let version = match params.get("protocolVersion").and_then(Value::as_str) {
        Some(version @ (V_NOV | V_JUN | V_MAR)) => version,
        _ => V_NOV,
    };
    rpc_ok(
        id,
        json!({
            "protocolVersion": version,
            "capabilities": { "tools": { "listChanged": false } },
            "serverInfo": { "name": SERVER_NAME, "version": SERVER_VERSION },
            "instructions": INSTRUCTIONS,
        }),
    )
}

fn discover(id: Value) -> Response {
    rpc_ok(
        id,
        json!({
            "resultType": "complete",
            "supportedVersions": [V_MODERN, V_NOV, V_JUN, V_MAR],
            "capabilities": { "tools": {} },
            "_meta": {
                META_SERVER: { "name": SERVER_NAME, "version": SERVER_VERSION }
            },
            "instructions": INSTRUCTIONS,
            "ttlMs": TTL_MS,
            "cacheScope": "public",
        }),
    )
}

fn tools_list(id: Value, params: &Value, modern: bool) -> Response {
    if let Some(cursor) = params.get("cursor") {
        let empty = cursor.as_str().is_some_and(str::is_empty);
        if !empty && !cursor.is_null() {
            return invalid_params(id, "unknown cursor");
        }
    }
    let mut result = json!({ "tools": tool_catalog() });
    if modern {
        result["resultType"] = json!("complete");
        result["ttlMs"] = json!(TTL_MS);
        result["cacheScope"] = json!("public");
    }
    rpc_ok(id, result)
}

fn tool_catalog() -> Value {
    json!([
        {
            "name": "query",
            "description": "Run one SQL request. The text is the same JSON envelope as POST /query.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "sql": { "type": "string" },
                    "params": {
                        "type": "array",
                        "description": "Null, text, integer, or a finite number."
                    }
                },
                "required": ["sql"],
                "additionalProperties": false
            }
        },
        {
            "name": "list_tables",
            "description": "List tables and views. Skips sqlite_% and osd1_applied.",
            "inputSchema": {
                "type": "object",
                "properties": {},
                "additionalProperties": false
            },
            "annotations": { "readOnlyHint": true, "destructiveHint": false }
        },
        {
            "name": "describe_table",
            "description": "Read one table or view by name. The first result is the CREATE text. The second is the columns. The third is the indexes. An unknown name returns no rows.",
            "inputSchema": {
                "type": "object",
                "properties": { "name": { "type": "string" } },
                "required": ["name"],
                "additionalProperties": false
            },
            "annotations": { "readOnlyHint": true, "destructiveHint": false }
        },
        {
            "name": "create_table",
            "description": "Create a table. A type is INTEGER, REAL, TEXT, BLOB, or NUMERIC.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "name": { "type": "string" },
                    "columns": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": { "type": "string" },
                                "type": { "type": "string" },
                                "primary_key": { "type": "boolean" },
                                "not_null": { "type": "boolean" }
                            },
                            "required": ["name"]
                        }
                    }
                },
                "required": ["name", "columns"],
                "additionalProperties": false
            },
            "annotations": { "readOnlyHint": false, "destructiveHint": false }
        },
        {
            "name": "alter_table",
            "description": "add_column, rename_column, rename_table, or drop_column.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "name": { "type": "string" },
                    "action": { "type": "string" },
                    "column": { "type": "string" },
                    "new_name": { "type": "string" },
                    "type": { "type": "string" },
                    "not_null": { "type": "boolean" }
                },
                "required": ["name", "action"],
                "additionalProperties": false
            },
            "annotations": { "readOnlyHint": false, "destructiveHint": true }
        },
        {
            "name": "drop_table",
            "description": "Drop a table. A missing table is an error. osd1_applied is reserved.",
            "inputSchema": {
                "type": "object",
                "properties": { "name": { "type": "string" } },
                "required": ["name"],
                "additionalProperties": false
            },
            "annotations": { "readOnlyHint": false, "destructiveHint": true }
        }
    ])
}

async fn call_tool(
    state: &AppState,
    headers: &HeaderMap,
    id: Value,
    params: &Value,
    modern: bool,
) -> Response {
    let name = params.get("name").and_then(Value::as_str);
    if modern {
        let header_name = header_text(headers, "mcp-name");
        if header_name.is_none() || header_name.as_deref() != name {
            return header_mismatch(id);
        }
    }
    let Some(name) = name else {
        return invalid_params(id, "tool name is required");
    };
    let arguments = match params.get("arguments") {
        None | Some(Value::Null) => json!({}),
        Some(value) if value.is_object() => value.clone(),
        Some(_) => return invalid_params(id, "arguments must be an object"),
    };
    let request = match name {
        "query" => match query_request(&arguments) {
            Ok(request) => request,
            Err(message) => return invalid_params(id, &message),
        },
        "list_tables" => SqlRequest {
            sql: LIST_TABLES_SQL.to_string(),
            params: Vec::new(),
            shape: ResultShape::Objects,
            deadline: None,
            min_commit_index: None,
        },
        "describe_table" => match describe_request(&arguments) {
            Ok(request) => request,
            Err(message) => return invalid_params(id, &message),
        },
        "create_table" => match create_sql(&arguments) {
            Ok(sql) => sql_request(sql),
            Err(message) => return invalid_params(id, &message),
        },
        "alter_table" => match alter_sql(&arguments) {
            Ok(sql) => sql_request(sql),
            Err(message) => return invalid_params(id, &message),
        },
        "drop_table" => match drop_sql(&arguments) {
            Ok(sql) => sql_request(sql),
            Err(message) => return invalid_params(id, &message),
        },
        _ => return invalid_params(id, "unknown tool"),
    };
    run_sql(state, headers, id, request).await
}

fn sql_request(sql: String) -> SqlRequest {
    SqlRequest {
        sql,
        params: Vec::new(),
        shape: ResultShape::Objects,
        deadline: None,
        min_commit_index: None,
    }
}

async fn run_sql(
    state: &AppState,
    headers: &HeaderMap,
    id: Value,
    request: SqlRequest,
) -> Response {
    if state.local_database_id.is_none() && !state.routes.is_empty() {
        return tool_result(id, "the router does not execute SQL", true, None);
    }
    let response = execute_local(state, headers, request).await;
    let status = response.status();
    if status == StatusCode::UNAUTHORIZED || status == StatusCode::SERVICE_UNAVAILABLE {
        return response;
    }
    let limit = state.max_response_bytes.saturating_add(64 * 1024);
    let bytes = match axum::body::to_bytes(response.into_body(), limit).await {
        Ok(bytes) => bytes,
        Err(_) => {
            return tool_result(id, "response exceeds the size limit", true, None);
        }
    };
    let parsed = serde_json::from_slice::<Value>(&bytes).ok();
    if status == StatusCode::OK {
        let text = String::from_utf8_lossy(&bytes).into_owned();
        return tool_result(id, &text, false, parsed);
    }
    let message = parsed
        .as_ref()
        .and_then(|value| value.pointer("/errors/0/message"))
        .and_then(Value::as_str)
        .unwrap_or("sql request failed")
        .to_string();
    tool_result(id, &message, true, parsed)
}

fn describe_request(arguments: &Value) -> Result<SqlRequest, String> {
    let name = required_str(arguments, "name")?;
    table_name(name)?;
    Ok(SqlRequest {
        sql: DESCRIBE_TABLE_SQL.to_string(),
        params: vec![SqlValue::Text(name.to_string())],
        shape: ResultShape::Objects,
        deadline: None,
        min_commit_index: None,
    })
}

fn query_request(arguments: &Value) -> Result<SqlRequest, String> {
    let sql = required_str(arguments, "sql")?;
    if sql.is_empty() {
        return Err("sql is empty".to_string());
    }
    let params =
        parse_params(arguments.get("params").cloned()).map_err(|error| error.to_string())?;
    Ok(SqlRequest {
        sql: sql.to_string(),
        params,
        shape: ResultShape::Objects,
        deadline: None,
        min_commit_index: None,
    })
}

fn create_sql(arguments: &Value) -> Result<String, String> {
    let table = table_name(required_str(arguments, "name")?)?;
    let columns = arguments
        .get("columns")
        .and_then(Value::as_array)
        .ok_or_else(|| "columns is required".to_string())?;
    if columns.is_empty() {
        return Err("at least one column is required".to_string());
    }
    let mut parts = Vec::with_capacity(columns.len());
    let mut primary_keys = 0;
    for column in columns {
        if !column.is_object() {
            return Err("column must be an object".to_string());
        }
        let name = quote_ident(required_str(column, "name")?)?;
        let kind = column_type(column.get("type").and_then(Value::as_str))?;
        let primary = flag(column, "primary_key")?;
        let not_null = flag(column, "not_null")?;
        if primary {
            primary_keys += 1;
        }
        let mut part = name;
        if !kind.is_empty() {
            part.push(' ');
            part.push_str(&kind);
        }
        if primary {
            part.push_str(" PRIMARY KEY");
        }
        if not_null {
            part.push_str(" NOT NULL");
        }
        parts.push(part);
    }
    if primary_keys > 1 {
        return Err("at most one primary key".to_string());
    }
    Ok(format!("CREATE TABLE {table} ({})", parts.join(", ")))
}

fn alter_sql(arguments: &Value) -> Result<String, String> {
    let table = table_name(required_str(arguments, "name")?)?;
    match required_str(arguments, "action")? {
        "add_column" => {
            let column = quote_ident(required_str(arguments, "column")?)?;
            let kind = column_type(arguments.get("type").and_then(Value::as_str))?;
            let not_null = flag(arguments, "not_null")?;
            let mut sql = format!("ALTER TABLE {table} ADD COLUMN {column}");
            if !kind.is_empty() {
                sql.push(' ');
                sql.push_str(&kind);
            }
            if not_null {
                sql.push_str(" NOT NULL");
            }
            Ok(sql)
        }
        "rename_column" => {
            let column = quote_ident(required_str(arguments, "column")?)?;
            let new_name = quote_ident(required_str(arguments, "new_name")?)?;
            Ok(format!(
                "ALTER TABLE {table} RENAME COLUMN {column} TO {new_name}"
            ))
        }
        "rename_table" => {
            let new_name = table_name(required_str(arguments, "new_name")?)?;
            Ok(format!("ALTER TABLE {table} RENAME TO {new_name}"))
        }
        "drop_column" => {
            let column = quote_ident(required_str(arguments, "column")?)?;
            Ok(format!("ALTER TABLE {table} DROP COLUMN {column}"))
        }
        _ => Err(
            "action must be add_column, rename_column, rename_table, or drop_column".to_string(),
        ),
    }
}

fn drop_sql(arguments: &Value) -> Result<String, String> {
    let table = table_name(required_str(arguments, "name")?)?;
    Ok(format!("DROP TABLE {table}"))
}

fn table_name(name: &str) -> Result<String, String> {
    if name == "osd1_applied" {
        return Err("osd1_applied is reserved".to_string());
    }
    quote_ident(name)
}

fn quote_ident(name: &str) -> Result<String, String> {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return Err("invalid identifier".to_string());
    };
    if !(first.is_ascii_alphabetic() || first == '_')
        || !chars.all(|char| char.is_ascii_alphanumeric() || char == '_')
    {
        return Err("invalid identifier".to_string());
    }
    Ok(format!("\"{name}\""))
}

fn column_type(value: Option<&str>) -> Result<String, String> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(String::new());
    };
    match value.to_ascii_uppercase().as_str() {
        "INTEGER" | "REAL" | "TEXT" | "BLOB" | "NUMERIC" => Ok(value.to_ascii_uppercase()),
        _ => Err("column type must be INTEGER, REAL, TEXT, BLOB, or NUMERIC".to_string()),
    }
}

fn flag(value: &Value, key: &str) -> Result<bool, String> {
    match value.get(key) {
        None | Some(Value::Null) => Ok(false),
        Some(Value::Bool(flag)) => Ok(*flag),
        Some(_) => Err(format!("{key} must be a boolean")),
    }
}

fn required_str<'a>(value: &'a Value, key: &str) -> Result<&'a str, String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{key} is required"))
}

fn content_type_is_json(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(';')
                .next()
                .unwrap_or("")
                .trim()
                .eq_ignore_ascii_case("application/json")
        })
}

fn accepts_json(headers: &HeaderMap) -> bool {
    let Some(value) = headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
    else {
        return true;
    };
    value.split(',').any(|part| {
        let mime = part.split(';').next().unwrap_or("").trim();
        mime.eq_ignore_ascii_case("application/json") || mime == "*/*"
    })
}

fn header_text(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

fn meta_version(message: &serde_json::Map<String, Value>) -> Result<Option<String>, ()> {
    let top = message
        .get("_meta")
        .and_then(|meta| meta.get(META_VERSION))
        .and_then(Value::as_str);
    let nested = message
        .get("params")
        .and_then(|params| params.get("_meta"))
        .and_then(|meta| meta.get(META_VERSION))
        .and_then(Value::as_str);
    match (top, nested) {
        (Some(top), Some(nested)) if top != nested => Err(()),
        (Some(version), _) | (_, Some(version)) => Ok(Some(version.to_string())),
        (None, None) => Ok(None),
    }
}

fn known_version(version: &str) -> bool {
    matches!(version, V_MODERN | V_NOV | V_JUN | V_MAR)
}

fn valid_id(id: &Value) -> bool {
    matches!(id, Value::Null | Value::String(_) | Value::Number(_))
}

fn tool_result(id: Value, text: &str, is_error: bool, structured: Option<Value>) -> Response {
    let mut result = json!({
        "content": [{ "type": "text", "text": text }],
        "isError": is_error,
    });
    if let Some(structured) = structured {
        result["structuredContent"] = structured;
    }
    rpc_ok(id, result)
}

fn rpc_ok(id: Value, result: Value) -> Response {
    json_response(
        StatusCode::OK,
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": result,
        }),
    )
}

fn invalid_params(id: Value, message: &str) -> Response {
    rpc_error(StatusCode::OK, id, -32602, message, None)
}

fn header_mismatch(id: Value) -> Response {
    rpc_error(
        StatusCode::BAD_REQUEST,
        id,
        -32020,
        "protocol header mismatch",
        None,
    )
}

fn unsupported(id: Value, requested: &str) -> Response {
    rpc_error(
        StatusCode::BAD_REQUEST,
        id,
        -32022,
        "Unsupported protocol version",
        Some(json!({
            "supported": [V_MODERN, V_NOV, V_JUN, V_MAR],
            "requested": requested,
        })),
    )
}

fn rpc_error(
    status: StatusCode,
    id: Value,
    code: i64,
    message: &str,
    data: Option<Value>,
) -> Response {
    let mut error = json!({ "code": code, "message": message });
    if let Some(data) = data {
        error["data"] = data;
    }
    json_response(
        status,
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": error,
        }),
    )
}
