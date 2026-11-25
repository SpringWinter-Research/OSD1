//! D1 REST `/query` and `/raw` HTTP adapter, plus the table explorer page.

mod metrics;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, Request, State};
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use metrics::{route_label, Metrics, SqlObservation, APPLY_LAG_NOT_READY};
use osd1_types::{
    parse_params, row_to_object, EnqueueResult, ResultShape, SqlError, SqlRequest, SqlResponse,
    SqlValue, StatementResult, ERROR_BAD_REQUEST, ERROR_QUEUE_FULL, ERROR_SQL, ERROR_UNAUTHORIZED,
};
use serde::Serialize;
use serde_json::Value;

/// `api_token` of `None` leaves `/query` and `/raw` unauthenticated.
///
/// `max_body_bytes` is enforced before SQL is queued. `max_response_bytes` rejects
/// a success envelope that would be larger than the limit. `request_timeout` covers
/// queue wait and SQL. `None` waits until the worker replies.
#[derive(Clone)]
pub struct AppState {
    pub executor: std::sync::Arc<dyn osd1_types::SqlExecutor>,
    pub api_token: Option<String>,
    pub max_body_bytes: usize,
    pub max_response_bytes: usize,
    pub request_timeout: Option<Duration>,
    pub metrics: Metrics,
    /// Set when this process is leaving the cluster. Queries stop, `/health` fails.
    pub draining: Arc<AtomicBool>,
    /// When set, this process serves only this database id from its own file.
    pub local_database_id: Option<String>,
    /// database id to `http://host:port` of the process that owns that file.
    pub routes: HashMap<String, String>,
}

impl AppState {
    pub fn new(
        executor: std::sync::Arc<dyn osd1_types::SqlExecutor>,
        api_token: Option<String>,
    ) -> Self {
        Self {
            executor,
            api_token,
            max_body_bytes: 1024 * 1024,
            max_response_bytes: 1024 * 1024,
            request_timeout: Some(Duration::from_secs(30)),
            metrics: Metrics::new(),
            draining: Arc::new(AtomicBool::new(false)),
            local_database_id: None,
            routes: HashMap::new(),
        }
    }
}

/// Routes `GET /`, `GET /explorer`, `GET /health`, `GET /metrics`,
/// `POST /accounts/{account_id}/d1/database/{database_id}/query`, and the matching `/raw` route.
///
/// `/`, `/explorer`, `/health`, and `/metrics` do not enqueue and do not use the bearer token.
/// Path ids are ignored unless `local_database_id` or `routes` is set.
/// The explorer page calls `/raw` with the token.
pub fn router(state: AppState) -> Router {
    let max_body_bytes = state.max_body_bytes;
    let metrics = state.metrics.clone();
    Router::new()
        .route("/", get(explorer))
        .route("/explorer", get(explorer))
        .route("/health", get(health))
        .route("/metrics", get(metrics_endpoint))
        .route(
            "/accounts/{account_id}/d1/database/{database_id}/query",
            post(query),
        )
        .route(
            "/accounts/{account_id}/d1/database/{database_id}/raw",
            post(raw),
        )
        .layer(DefaultBodyLimit::max(max_body_bytes))
        .layer(middleware::from_fn(move |request: Request, next: Next| {
            let metrics = metrics.clone();
            async move {
                let route = route_label(request.uri().path());
                let started = Instant::now();
                let response = next.run(request).await;
                metrics.observe_http(
                    route,
                    response.status().as_u16(),
                    started.elapsed().as_secs_f64(),
                );
                response
            }
        }))
        .with_state(state)
}

const EXPLORER_HTML: &str = include_str!("../static/explorer.html");

async fn explorer() -> Response {
    (
        StatusCode::OK,
        [
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("text/html; charset=utf-8"),
            ),
            (header::CACHE_CONTROL, HeaderValue::from_static("no-cache")),
        ],
        EXPLORER_HTML,
    )
        .into_response()
}

async fn health(State(state): State<AppState>) -> Response {
    let draining = state.draining.load(Ordering::Relaxed);
    let ready = !draining && state.metrics.apply_lag() <= APPLY_LAG_NOT_READY;
    let status = if draining {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    };
    json_response(
        status,
        HealthBody {
            ok: !draining,
            ready,
        },
    )
}

async fn metrics_endpoint(State(state): State<AppState>) -> Response {
    state.metrics.refresh_litestream();
    state.metrics.set_queue(
        state.executor.sql_inflight(),
        state.executor.sql_queue_capacity(),
    );
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
        )],
        state.metrics.encode(),
    )
        .into_response()
}

async fn query(
    State(state): State<AppState>,
    Path((account, database)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    dispatch(
        &state,
        &account,
        &database,
        &headers,
        &body,
        ResultShape::Objects,
    )
    .await
}

async fn raw(
    State(state): State<AppState>,
    Path((account, database)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    dispatch(
        &state,
        &account,
        &database,
        &headers,
        &body,
        ResultShape::Raw,
    )
    .await
}

async fn dispatch(
    state: &AppState,
    account: &str,
    database: &str,
    headers: &HeaderMap,
    body: &[u8],
    shape: ResultShape,
) -> Response {
    match route_target(state, database) {
        Target::Local => handle(state, headers, body, shape).await,
        Target::Proxy(upstream) => {
            if let Err(response) = authorize(state, headers) {
                return *response;
            }
            proxy(state, &upstream, account, database, headers, body, shape).await
        }
        Target::Unknown => fail(
            StatusCode::NOT_FOUND,
            ERROR_BAD_REQUEST,
            format!("unknown database {database}"),
        ),
    }
}

enum Target {
    Local,
    Proxy(String),
    Unknown,
}

fn route_target(state: &AppState, database: &str) -> Target {
    if state.routes.is_empty() && state.local_database_id.is_none() {
        return Target::Local;
    }
    if state.local_database_id.as_deref() == Some(database) {
        return Target::Local;
    }
    if let Some(upstream) = state.routes.get(database) {
        return Target::Proxy(upstream.clone());
    }
    Target::Unknown
}

async fn proxy(
    state: &AppState,
    upstream: &str,
    account: &str,
    database: &str,
    headers: &HeaderMap,
    body: &[u8],
    shape: ResultShape,
) -> Response {
    let Some((host, port)) = parse_upstream(upstream) else {
        return fail(
            StatusCode::BAD_GATEWAY,
            ERROR_BAD_REQUEST,
            "route upstream must be http://host:port",
        );
    };
    if account.contains(['/', '?', ' ', '\r', '\n'])
        || database.contains(['/', '?', ' ', '\r', '\n'])
    {
        return fail(
            StatusCode::BAD_REQUEST,
            ERROR_BAD_REQUEST,
            "database id is not a single path segment",
        );
    }
    let kind = match shape {
        ResultShape::Objects => "query",
        ResultShape::Raw => "raw",
    };
    let path = format!("/accounts/{account}/d1/database/{database}/{kind}");
    match forward(&host, port, &path, headers, body, state.max_response_bytes).await {
        Ok((status, bytes)) => {
            let code = StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY);
            (
                code,
                [(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                )],
                bytes,
            )
                .into_response()
        }
        Err(error) => fail(StatusCode::BAD_GATEWAY, ERROR_BAD_REQUEST, error),
    }
}

fn parse_upstream(upstream: &str) -> Option<(String, u16)> {
    let rest = upstream.strip_prefix("http://")?;
    if rest.contains('/') {
        return None;
    }
    let (host, port) = rest.split_once(':')?;
    if host.is_empty() {
        return None;
    }
    Some((host.to_string(), port.parse().ok()?))
}

async fn forward(
    host: &str,
    port: u16,
    path: &str,
    headers: &HeaderMap,
    body: &[u8],
    max_response_bytes: usize,
) -> Result<(u16, Vec<u8>), String> {
    let mut stream = tokio::net::TcpStream::connect((host, port))
        .await
        .map_err(|error| error.to_string())?;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut request = format!(
        "POST {path} HTTP/1.1\r\nHost: {host}:{port}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    if let Some(value) = headers.get(header::AUTHORIZATION) {
        let value = value
            .to_str()
            .map_err(|_| "authorization header".to_string())?;
        request.push_str("Authorization: ");
        request.push_str(value);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|error| error.to_string())?;
    stream
        .write_all(body)
        .await
        .map_err(|error| error.to_string())?;
    let mut received = Vec::new();
    let mut buf = [0u8; 8192];
    loop {
        let n = stream
            .read(&mut buf)
            .await
            .map_err(|error| error.to_string())?;
        if n == 0 {
            break;
        }
        if received.len() + n > max_response_bytes + 64 * 1024 {
            return Err("upstream response is too large".into());
        }
        received.extend_from_slice(&buf[..n]);
    }
    let text = String::from_utf8_lossy(&received);
    let (head, payload) = text
        .split_once("\r\n\r\n")
        .ok_or_else(|| "upstream response has no header".to_string())?;
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or_else(|| "upstream status".to_string())?;
    let payload = payload.as_bytes().to_vec();
    if payload.len() > max_response_bytes {
        return Err("upstream response is too large".into());
    }
    Ok((status, payload))
}

async fn handle(
    state: &AppState,
    headers: &HeaderMap,
    body: &[u8],
    shape: ResultShape,
) -> Response {
    if state.draining.load(Ordering::Relaxed) {
        return leaving();
    }
    if let Err(response) = authorize(state, headers) {
        return *response;
    }
    let mut request = match parse_request(body, shape) {
        Ok(request) => request,
        Err(response) => return *response,
    };
    if let Some(limit) = state.request_timeout {
        request.deadline = Some(Instant::now() + limit);
    }
    if let Err(response) = apply_min_commit_index(&mut request, headers) {
        return *response;
    }
    match state.executor.enqueue(request) {
        EnqueueResult::Accepted(receiver) => wait_for_sql(state, receiver, shape).await,
        EnqueueResult::Full => {
            state.metrics.observe_sql("queue_full", shape, None);
            queue_full()
        }
        EnqueueResult::Closed => {
            state.metrics.observe_sql("shut_down", shape, None);
            shut_down()
        }
    }
}

async fn wait_for_sql(
    state: &AppState,
    receiver: tokio::sync::oneshot::Receiver<Result<SqlResponse, SqlError>>,
    shape: ResultShape,
) -> Response {
    let pending = async {
        match receiver.await {
            Ok(Ok(response)) => limited_success(state, &response, shape),
            Ok(Err(SqlError::ShuttingDown)) => {
                state.metrics.observe_sql("shut_down", shape, None);
                shut_down()
            }
            Ok(Err(SqlError::QueueFull)) => {
                state.metrics.observe_sql("queue_full", shape, None);
                queue_full()
            }
            Ok(Err(SqlError::TimedOut)) => {
                state.metrics.observe_sql("timeout", shape, None);
                request_timed_out()
            }
            Ok(Err(error)) => {
                state.metrics.observe_sql("sql_error", shape, None);
                fail(StatusCode::BAD_REQUEST, ERROR_SQL, error.to_string())
            }
            // The worker dropped the response channel.
            Err(_recv) => {
                state.metrics.observe_sql("shut_down", shape, None);
                shut_down()
            }
        }
    };
    match state.request_timeout {
        Some(limit) => match tokio::time::timeout(limit, pending).await {
            Ok(response) => response,
            Err(_elapsed) => {
                state.metrics.observe_sql("timeout", shape, None);
                request_timed_out()
            }
        },
        None => pending.await,
    }
}

fn limited_success(state: &AppState, response: &SqlResponse, shape: ResultShape) -> Response {
    let observation = observation_from(response);
    let bytes = serde_json::to_vec(&success_body(response, shape)).expect("envelope is json");
    if bytes.len() > state.max_response_bytes {
        state
            .metrics
            .observe_sql("response_too_large", shape, Some(observation));
        return fail(
            StatusCode::BAD_REQUEST,
            ERROR_SQL,
            "response exceeds the size limit",
        );
    }
    state.metrics.observe_sql("ok", shape, Some(observation));
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    if let Some(index) = response.commit_index {
        headers.insert(
            HeaderName::from_static("x-osd1-commit-index"),
            HeaderValue::from_str(&index.to_string()).expect("commit index is digits"),
        );
    }
    (StatusCode::OK, headers, bytes).into_response()
}

fn apply_min_commit_index(
    request: &mut SqlRequest,
    headers: &HeaderMap,
) -> Result<(), Box<Response>> {
    let Some(value) = headers.get("x-osd1-min-commit-index") else {
        return Ok(());
    };
    let text = value.to_str().unwrap_or("");
    match text.parse::<u64>() {
        Ok(index) if index >= 1 => {
            request.min_commit_index = Some(index);
            Ok(())
        }
        _ => Err(Box::new(bad_request(
            "x-osd1-min-commit-index must be an integer of 1 or more",
        ))),
    }
}

fn observation_from(response: &SqlResponse) -> SqlObservation {
    let mut seconds = 0.0;
    let mut rows_read = 0;
    let mut rows_written = 0;
    let mut database_size_bytes = 0;
    for statement in &response.statements {
        seconds += statement.meta.duration_ms / 1000.0;
        rows_read += statement.meta.rows_read;
        rows_written += statement.meta.rows_written;
        database_size_bytes = statement.meta.size_after;
    }
    SqlObservation {
        seconds,
        statements: response.statements.len() as u64,
        rows_read,
        rows_written,
        database_size_bytes,
    }
}

fn authorize(state: &AppState, headers: &HeaderMap) -> Result<(), Box<Response>> {
    let Some(expected) = state.api_token.as_deref() else {
        return Ok(());
    };
    let allowed = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| bearer_matches(value, expected));
    if allowed {
        Ok(())
    } else {
        Err(Box::new(fail(
            StatusCode::UNAUTHORIZED,
            ERROR_UNAUTHORIZED,
            "unauthorized",
        )))
    }
}

fn bearer_matches(value: &str, expected: &str) -> bool {
    let Some((scheme, token)) = value.trim().split_once(char::is_whitespace) else {
        return false;
    };
    scheme.eq_ignore_ascii_case("bearer") && token.trim() == expected
}

fn parse_request(body: &[u8], shape: ResultShape) -> Result<SqlRequest, Box<Response>> {
    let value: Value = match serde_json::from_slice(body) {
        Ok(value) => value,
        Err(_) => return Err(Box::new(bad_request("malformed json"))),
    };
    let Some(object) = value.as_object() else {
        return Err(Box::new(bad_request("sql is required")));
    };
    let Some(sql) = object.get("sql").and_then(Value::as_str) else {
        return Err(Box::new(bad_request("sql is required")));
    };
    // Only an empty string is rejected. Surrounding whitespace is part of the SQL.
    if sql.is_empty() {
        return Err(Box::new(bad_request("sql is empty")));
    }
    let params = match parse_params(object.get("params").cloned()) {
        Ok(params) => params,
        Err(error) => return Err(Box::new(bad_request(error.to_string()))),
    };
    Ok(SqlRequest {
        sql: sql.to_owned(),
        params,
        shape,
        deadline: None,
        min_commit_index: None,
    })
}

fn success_body(response: &SqlResponse, shape: ResultShape) -> Envelope<Vec<StatementEnvelope>> {
    Envelope {
        success: true,
        errors: Vec::new(),
        messages: Vec::new(),
        result: response
            .statements
            .iter()
            .map(|statement| statement_envelope(statement, shape))
            .collect(),
    }
}

fn statement_envelope(statement: &StatementResult, shape: ResultShape) -> StatementEnvelope {
    let meta = &statement.meta;
    StatementEnvelope {
        success: statement.success,
        results: results_value(statement, shape),
        meta: MetaEnvelope {
            changed_db: meta.changed_db,
            changes: meta.changes,
            duration: meta.duration_ms,
            last_row_id: meta.last_row_id,
            rows_read: meta.rows_read,
            rows_written: meta.rows_written,
            served_by_colo: meta.served_by_colo.as_ref().to_owned(),
            served_by_primary: meta.served_by_primary,
            served_by_region: meta.served_by_region.as_ref().to_owned(),
            size_after: meta.size_after,
            timings: TimingsEnvelope {
                sql_duration_ms: meta.duration_ms,
            },
        },
    }
}

fn results_value(statement: &StatementResult, shape: ResultShape) -> Value {
    match shape {
        ResultShape::Objects => Value::Array(
            statement
                .rows
                .iter()
                .map(|row| row_to_object(&statement.columns, row))
                .collect(),
        ),
        ResultShape::Raw => serde_json::to_value(RawResults {
            columns: statement.columns.iter().cloned().collect(),
            rows: statement
                .rows
                .iter()
                .map(|row| row.iter().map(SqlValue::to_json).collect())
                .collect(),
        })
        .expect("raw results are json"),
    }
}

fn bad_request(message: impl Into<String>) -> Response {
    fail(StatusCode::BAD_REQUEST, ERROR_BAD_REQUEST, message)
}

fn leaving() -> Response {
    fail(
        StatusCode::SERVICE_UNAVAILABLE,
        ERROR_SQL,
        "node is leaving the cluster",
    )
}

fn shut_down() -> Response {
    fail(
        StatusCode::INTERNAL_SERVER_ERROR,
        ERROR_SQL,
        "sql worker is shut down",
    )
}

fn request_timed_out() -> Response {
    let mut response = fail(
        StatusCode::GATEWAY_TIMEOUT,
        ERROR_SQL,
        "sql request timed out",
    );
    response
        .headers_mut()
        .insert("x-should-retry", HeaderValue::from_static("false"));
    response
}

fn queue_full() -> Response {
    let mut response = fail(
        StatusCode::TOO_MANY_REQUESTS,
        ERROR_QUEUE_FULL,
        "sql queue is full",
    );
    // cloudflare@4.5.0 retries 429 unless this header is false.
    response
        .headers_mut()
        .insert("x-should-retry", HeaderValue::from_static("false"));
    response
}

fn fail(status: StatusCode, code: i64, message: impl Into<String>) -> Response {
    json_response(
        status,
        Envelope {
            success: false,
            errors: vec![ApiError {
                code,
                message: message.into(),
            }],
            messages: Vec::new(),
            result: Vec::<StatementEnvelope>::new(),
        },
    )
}

fn json_response(status: StatusCode, body: impl Serialize) -> Response {
    (status, Json(body)).into_response()
}

#[derive(Serialize)]
struct HealthBody {
    ok: bool,
    ready: bool,
}

#[derive(Serialize)]
struct Envelope<T> {
    success: bool,
    errors: Vec<ApiError>,
    messages: Vec<Value>,
    result: T,
}

#[derive(Serialize)]
struct ApiError {
    code: i64,
    message: String,
}

#[derive(Serialize)]
struct StatementEnvelope {
    success: bool,
    results: Value,
    meta: MetaEnvelope,
}

#[derive(Serialize)]
struct MetaEnvelope {
    changed_db: bool,
    changes: u64,
    duration: f64,
    last_row_id: i64,
    rows_read: u64,
    rows_written: u64,
    served_by_colo: String,
    served_by_primary: bool,
    served_by_region: String,
    size_after: u64,
    timings: TimingsEnvelope,
}

#[derive(Serialize)]
struct TimingsEnvelope {
    sql_duration_ms: f64,
}

#[derive(Serialize)]
struct RawResults {
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::sync::{Arc, Mutex};

    use axum::body::Body;
    use axum::http::{header, HeaderMap, HeaderValue, Request, StatusCode};
    use http_body_util::BodyExt;
    use osd1_types::{
        EnqueueResult, ResultShape, SqlError, SqlExecutor, SqlRequest, SqlResponse, SqlValue,
        StatementMeta, StatementResult,
    };
    use serde_json::{json, Value};
    use tower::ServiceExt;

    use std::time::{Duration, Instant};

    use super::{router, AppState};

    const QUERY: &str = "/accounts/023e105f4ecef8ad9ca31a8372d0c353/d1/database/db-id/query";
    const RAW: &str = "/accounts/acc/d1/database/db/raw";

    enum Outcome {
        Panic,
        Full,
        Closed,
        Canceled,
        Pending,
        Ready(Box<dyn Fn() -> Result<SqlResponse, SqlError> + Send + Sync>),
    }

    struct Fake {
        requests: Mutex<Vec<SqlRequest>>,
        outcome: Outcome,
    }

    impl SqlExecutor for Fake {
        fn enqueue(&self, request: SqlRequest) -> EnqueueResult {
            let outcome = match &self.outcome {
                Outcome::Panic => panic!("enqueue must not be called"),
                Outcome::Full => EnqueueResult::Full,
                Outcome::Closed => EnqueueResult::Closed,
                Outcome::Canceled => {
                    let (_sender, receiver) = tokio::sync::oneshot::channel();
                    EnqueueResult::Accepted(receiver)
                }
                Outcome::Pending => {
                    let (sender, receiver) = tokio::sync::oneshot::channel();
                    std::mem::forget(sender);
                    EnqueueResult::Accepted(receiver)
                }
                Outcome::Ready(respond) => {
                    let (sender, receiver) = tokio::sync::oneshot::channel();
                    let _ = sender.send(respond());
                    EnqueueResult::Accepted(receiver)
                }
            };
            self.requests.lock().unwrap().push(request);
            outcome
        }
    }

    fn ready_ok(statements: Vec<StatementResult>) -> Outcome {
        Outcome::Ready(Box::new(move || {
            Ok(SqlResponse {
                statements: statements.clone(),
                commit_index: None,
            })
        }))
    }

    fn ready_err(error: SqlError) -> Outcome {
        Outcome::Ready(Box::new(move || Err(duplicate_error(&error))))
    }

    fn duplicate_error(error: &SqlError) -> SqlError {
        match error {
            SqlError::Sqlite { code, message } => SqlError::Sqlite {
                code: *code,
                message: message.clone(),
            },
            SqlError::AuthorizerDenied { message } => SqlError::AuthorizerDenied {
                message: message.clone(),
            },
            SqlError::TransactionLeftOpen => SqlError::TransactionLeftOpen,
            SqlError::QueueFull => SqlError::QueueFull,
            SqlError::ShuttingDown => SqlError::ShuttingDown,
            SqlError::TimedOut => SqlError::TimedOut,
        }
    }

    fn app(outcome: Outcome, token: Option<&str>) -> (Arc<Fake>, axum::Router) {
        let fake = Arc::new(Fake {
            requests: Mutex::new(Vec::new()),
            outcome,
        });
        let executor: Arc<dyn SqlExecutor> = fake.clone();
        let app = router(AppState::new(executor, token.map(str::to_string)));
        (fake, app)
    }

    fn post(uri: &str, authorization: Option<&str>, body: &str) -> Request<Body> {
        let mut builder = Request::builder()
            .method("POST")
            .uri(uri)
            .header(header::CONTENT_TYPE, "application/json");
        if let Some(authorization) = authorization {
            builder = builder.header(header::AUTHORIZATION, authorization);
        }
        builder.body(Body::from(body.to_owned())).unwrap()
    }

    async fn dispatch(app: axum::Router, request: Request<Body>) -> (StatusCode, HeaderMap, Value) {
        let response = app.oneshot(request).await.expect("response");
        let status = response.status();
        let headers = response.headers().clone();
        assert_eq!(
            headers
                .get(header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok()),
            Some("application/json")
        );
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let body = serde_json::from_slice(&bytes).expect("json body");
        (status, headers, body)
    }

    fn sample_meta() -> StatementMeta {
        StatementMeta {
            changed_db: true,
            changes: 4,
            duration_ms: 1.5,
            last_row_id: 9,
            rows_read: 2,
            rows_written: 4,
            size_after: 11,
            served_by_colo: Arc::from("LOC"),
            served_by_region: Arc::from("WNAM"),
            served_by_primary: false,
        }
    }

    fn statement(column: &str, value: i64) -> StatementResult {
        StatementResult {
            columns: Arc::from(vec![column.to_string()]),
            rows: vec![vec![SqlValue::Integer(value)]],
            meta: sample_meta(),
            success: true,
        }
    }

    fn expected_meta() -> Value {
        json!({
            "changed_db": true,
            "changes": 4,
            "duration": 1.5,
            "last_row_id": 9,
            "rows_read": 2,
            "rows_written": 4,
            "served_by_colo": "LOC",
            "served_by_primary": false,
            "served_by_region": "WNAM",
            "size_after": 11,
            "timings": { "sql_duration_ms": 1.5 }
        })
    }

    fn expected_statement(results: Value) -> Value {
        json!({
            "success": true,
            "results": results,
            "meta": expected_meta(),
        })
    }

    fn assert_failure(body: &Value, code: i64, message: &str) {
        assert_eq!(
            body,
            &json!({
                "success": false,
                "errors": [{ "code": code, "message": message }],
                "messages": [],
                "result": []
            })
        );
    }

    #[tokio::test]
    async fn router_checks_the_token_then_proxies_and_rejects_an_unknown_id() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("upstream");
        let port = listener.local_addr().expect("port").port();
        let seen = std::sync::Arc::new(tokio::sync::Mutex::new(String::new()));
        let seen_task = std::sync::Arc::clone(&seen);
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut buf = vec![0u8; 8192];
            let n = stream.read(&mut buf).await.expect("read");
            *seen_task.lock().await = String::from_utf8_lossy(&buf[..n]).into_owned();
            let body = r#"{"success":true,"errors":[],"messages":[],"result":[]}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.expect("write");
        });

        let fake = std::sync::Arc::new(Fake {
            requests: Mutex::new(Vec::new()),
            outcome: ready_ok(vec![statement("n", 1)]),
        });
        let executor: std::sync::Arc<dyn SqlExecutor> = fake.clone();
        let mut state = AppState::new(executor, Some("secret".to_string()));
        state.local_database_id = Some("db-id".to_string());
        state
            .routes
            .insert("other-db".to_string(), format!("http://127.0.0.1:{port}"));
        let app = router(state);

        let (status, _, body) = dispatch(
            app.clone(),
            post(
                "/accounts/a/d1/database/other-db/query",
                None,
                r#"{"sql":"SELECT 1"}"#,
            ),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_failure(&body, 10000, "unauthorized");
        assert!(seen.lock().await.is_empty(), "token was not checked first");

        let (status, _, body) = dispatch(
            app.clone(),
            post(
                "/accounts/a/d1/database/missing/query",
                Some("Bearer secret"),
                r#"{"sql":"SELECT 1"}"#,
            ),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_failure(&body, 7400, "unknown database missing");
        assert!(seen.lock().await.is_empty());

        let (status, _, body) = dispatch(
            app.clone(),
            post(
                "/accounts/a/d1/database/other-db/query",
                Some("Bearer secret"),
                r#"{"sql":"SELECT 1"}"#,
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["success"], json!(true));
        let captured = seen.lock().await.clone();
        assert!(captured.contains("POST /accounts/a/d1/database/other-db/query "));
        assert!(captured.contains("Authorization: Bearer secret"));
        assert!(fake.requests.lock().unwrap().is_empty());

        let (status, _, _) = dispatch(
            app,
            post(QUERY, Some("Bearer secret"), r#"{"sql":"SELECT 1"}"#),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(fake.requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn metrics_are_prometheus_text_and_count_a_query() {
        let (_fake, app) = app(ready_ok(vec![statement("n", 1)]), Some("secret"));
        let (status, _, _) = dispatch(
            app.clone(),
            post(QUERY, Some("Bearer secret"), r#"{"sql":"SELECT 1"}"#),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("metrics");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok()),
            Some("text/plain; version=0.0.4; charset=utf-8")
        );
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(text.contains("osd1_build_info{version=\"0.1.0\"} 1"));
        assert!(text.contains("osd1_http_requests_total{route=\"query\",status=\"200\"} 1"));
        assert!(text.contains("osd1_sql_requests_total{result=\"ok\",route=\"query\"} 1"));
        assert!(text.contains("osd1_rows_read_total 2"));
        assert!(text.contains("osd1_rows_written_total 4"));
        assert!(text.contains("osd1_database_size_bytes 11"));
        assert!(text.contains("osd1_sql_queue_capacity 0"));
        assert!(!text.contains("SELECT"));
        assert!(!text.contains("secret"));
        assert!(!text.contains("osd1_raft"));
    }

    #[tokio::test]
    async fn health_does_not_enqueue() {
        let (_fake, app) = app(Outcome::Panic, Some("secret"));
        let (status, _, body) = dispatch(
            app,
            Request::builder()
                .method("GET")
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, json!({ "ok": true, "ready": true }));
    }

    #[tokio::test]
    async fn a_lagging_node_stays_up_and_a_draining_node_rejects_sql() {
        let fake = Arc::new(Fake {
            requests: Mutex::new(Vec::new()),
            outcome: Outcome::Panic,
        });
        let executor: Arc<dyn SqlExecutor> = fake.clone();
        let state = AppState::new(executor, Some("secret".to_string()));
        state.metrics.enable_raft();
        state.metrics.set_raft(3, 1, 100, 0, 2);
        let app = router(state.clone());
        let (status, _, body) = dispatch(
            app,
            Request::builder()
                .method("GET")
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, json!({ "ok": true, "ready": false }));

        state.draining.store(true, Ordering::Relaxed);
        let app = router(state);
        let (status, _, body) = dispatch(
            app.clone(),
            Request::builder()
                .method("GET")
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body, json!({ "ok": false, "ready": false }));
        let (status, _, body) = dispatch(
            app,
            post(
                "/accounts/a/d1/database/b/query",
                Some("Bearer secret"),
                r#"{"sql":"SELECT 1"}"#,
            ),
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["errors"][0]["message"], "node is leaving the cluster");
        assert!(fake.requests.lock().expect("requests").is_empty());
    }

    #[tokio::test]
    async fn explorer_is_unauthenticated_html() {
        let (fake, app) = app(Outcome::Panic, Some("secret"));
        for uri in ["/", "/explorer"] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("GET")
                        .uri(uri)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .expect("explorer");
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                response
                    .headers()
                    .get(header::CONTENT_TYPE)
                    .and_then(|value| value.to_str().ok()),
                Some("text/html; charset=utf-8")
            );
            assert_eq!(
                response
                    .headers()
                    .get(header::CACHE_CONTROL)
                    .and_then(|value| value.to_str().ok()),
                Some("no-cache")
            );
            let bytes = response.into_body().collect().await.unwrap().to_bytes();
            let text = String::from_utf8(bytes.to_vec()).unwrap();
            assert!(text.contains("API token"), "{uri}");
            assert!(
                text.contains("/accounts/local/d1/database/local/raw"),
                "{uri}"
            );
        }
        assert!(fake.requests.lock().unwrap().is_empty());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("metrics");
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(text.contains("route=\"explorer\""));
        assert!(!text.contains("secret"));
        assert!(!text.contains("SELECT"));
    }

    #[tokio::test]
    async fn query_returns_object_rows() {
        let (fake, app) = app(ready_ok(vec![statement("n", 1)]), None);
        let (status, _, body) = dispatch(
            app,
            post(
                QUERY,
                None,
                r#"{"sql":"SELECT 1 AS n","params":["a",null,2]}"#,
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body,
            json!({
                "success": true,
                "errors": [],
                "messages": [],
                "result": [expected_statement(json!([{ "n": 1 }]))]
            })
        );
        assert!(body["result"][0]["results"][0]["n"].is_i64());
        let recorded = fake.requests.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].sql, "SELECT 1 AS n");
        assert_eq!(recorded[0].shape, ResultShape::Objects);
        assert_eq!(
            recorded[0].params,
            vec![
                SqlValue::Text("a".to_string()),
                SqlValue::Null,
                SqlValue::Integer(2),
            ]
        );
    }

    #[tokio::test]
    async fn raw_returns_columns_and_rows() {
        let (fake, app) = app(ready_ok(vec![statement("n", 1)]), None);
        let (status, _, body) = dispatch(app, post(RAW, None, r#"{"sql":"SELECT 1 AS n"}"#)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body,
            json!({
                "success": true,
                "errors": [],
                "messages": [],
                "result": [expected_statement(json!({
                    "columns": ["n"],
                    "rows": [[1]]
                }))]
            })
        );
        assert!(body["result"][0]["results"]["rows"][0][0].is_i64());
        let recorded = fake.requests.lock().unwrap();
        assert_eq!(recorded[0].shape, ResultShape::Raw);
        assert!(recorded[0].params.is_empty());
    }

    #[tokio::test]
    async fn two_statements_become_two_result_elements() {
        let (_fake, app) = app(ready_ok(vec![statement("n", 1), statement("n", 2)]), None);
        let (status, _, body) =
            dispatch(app, post(QUERY, None, r#"{"sql":"SELECT 1; SELECT 2"}"#)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body["result"],
            json!([
                expected_statement(json!([{ "n": 1 }])),
                expected_statement(json!([{ "n": 2 }])),
            ])
        );
    }

    #[tokio::test]
    async fn missing_token_is_unauthorized() {
        let (fake, app) = app(ready_ok(vec![]), Some("test-token"));
        let (status, _, body) = dispatch(app, post(QUERY, None, r#"{"sql":"SELECT 1"}"#)).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_failure(&body, 10000, "unauthorized");
        assert!(fake.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn wrong_token_is_unauthorized() {
        let (fake, app) = app(ready_ok(vec![]), Some("test-token"));
        for authorization in [
            "Bearer wrong-token",
            "Bearer Test-Token",
            "Basic test-token",
        ] {
            let (status, _, body) = dispatch(
                app.clone(),
                post(QUERY, Some(authorization), r#"{"sql":"SELECT 1"}"#),
            )
            .await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{authorization}");
            assert_failure(&body, 10000, "unauthorized");
        }
        assert!(fake.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn min_commit_index_is_recorded_and_a_bad_value_is_rejected() {
        let (fake, ok_app) = app(ready_ok(vec![statement("n", 1)]), None);
        let mut request = post(QUERY, None, r#"{"sql":"SELECT 1"}"#);
        request
            .headers_mut()
            .insert("x-osd1-min-commit-index", HeaderValue::from_static("4"));
        let (status, _, _) = dispatch(ok_app, request).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(fake.requests.lock().unwrap()[0].min_commit_index, Some(4));

        let (fake, rejected) = app(Outcome::Panic, None);
        let mut request = post(QUERY, None, r#"{"sql":"SELECT 1"}"#);
        request
            .headers_mut()
            .insert("x-osd1-min-commit-index", HeaderValue::from_static("0"));
        let (status, _, body) = dispatch(rejected, request).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_failure(
            &body,
            7400,
            "x-osd1-min-commit-index must be an integer of 1 or more",
        );
        assert!(fake.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_commit_index_is_returned_as_a_header() {
        let (_fake, app) = app(
            Outcome::Ready(Box::new(|| {
                Ok(SqlResponse {
                    statements: vec![statement("n", 1)],
                    commit_index: Some(12),
                })
            })),
            None,
        );
        let (status, headers, body) =
            dispatch(app, post(QUERY, None, r#"{"sql":"SELECT 1"}"#)).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            headers
                .get("x-osd1-commit-index")
                .and_then(|value| value.to_str().ok()),
            Some("12")
        );
    }

    #[tokio::test]
    async fn configured_token_allows_the_request() {
        let (fake, app) = app(ready_ok(vec![statement("n", 1)]), Some("test-token"));
        for authorization in ["Bearer test-token", "bEaReR test-token"] {
            let (status, _, body) = dispatch(
                app.clone(),
                post(QUERY, Some(authorization), r#"{"sql":"SELECT 1"}"#),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{authorization}");
            assert_eq!(body["result"][0]["results"], json!([{ "n": 1 }]));
        }
        assert_eq!(fake.requests.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn open_router_allows_a_request_without_a_token() {
        let (fake, app) = app(ready_ok(vec![statement("n", 1)]), None);
        let (status, _, body) = dispatch(app, post(RAW, None, r#"{"sql":"SELECT 1"}"#)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["success"], json!(true));
        assert_eq!(fake.requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn full_queue_sets_should_retry_false() {
        let (_fake, app) = app(Outcome::Full, None);
        let (status, headers, body) =
            dispatch(app, post(QUERY, None, r#"{"sql":"SELECT 1"}"#)).await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            headers
                .get("x-should-retry")
                .and_then(|value| value.to_str().ok()),
            Some("false")
        );
        assert_failure(&body, 10001, "sql queue is full");
    }

    #[tokio::test]
    async fn sqlite_errors_use_code_7500() {
        let (_fake, app) = app(
            ready_err(SqlError::Sqlite {
                code: 1,
                message: "near \"NOPE\": syntax error".to_string(),
            }),
            None,
        );
        let (status, _, body) = dispatch(app, post(QUERY, None, r#"{"sql":"NOPE"}"#)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_failure(&body, 7500, "sqlite error 1: near \"NOPE\": syntax error");
    }

    #[tokio::test]
    async fn bool_params_are_rejected() {
        let (fake, app) = app(ready_ok(vec![]), None);
        let (status, _, body) = dispatch(
            app,
            post(QUERY, None, r#"{"sql":"SELECT ?","params":[true]}"#),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_failure(
            &body,
            7400,
            "parameter 0 must be null, a string, an integer, or a finite number",
        );
        assert!(fake.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn malformed_json_is_rejected() {
        let (fake, app) = app(ready_ok(vec![]), None);
        let (status, _, body) = dispatch(app, post(QUERY, None, "not-json")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_failure(&body, 7400, "malformed json");
        assert!(fake.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn missing_and_empty_sql_are_bad_requests() {
        let (fake, app) = app(ready_ok(vec![]), None);
        let (status, _, body) = dispatch(app.clone(), post(QUERY, None, "{}")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_failure(&body, 7400, "sql is required");
        let (status, _, body) = dispatch(app, post(RAW, None, r#"{"sql":""}"#)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_failure(&body, 7400, "sql is empty");
        assert!(fake.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn sql_is_not_trimmed() {
        let (fake, app) = app(ready_ok(vec![]), None);
        let (status, _, _) = dispatch(app, post(QUERY, None, r#"{"sql":"  SELECT 1  "}"#)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(fake.requests.lock().unwrap()[0].sql, "  SELECT 1  ");
    }

    #[tokio::test]
    async fn shut_down_is_code_7500() {
        let cases = [
            (Outcome::Closed, "closed"),
            (ready_err(SqlError::ShuttingDown), "shutting down"),
            (Outcome::Canceled, "canceled"),
        ];
        for (outcome, label) in cases {
            let (_fake, app) = app(outcome, None);
            let (status, _, body) = dispatch(app, post(QUERY, None, r#"{"sql":"SELECT 1"}"#)).await;
            assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{label}");
            assert_failure(&body, 7500, "sql worker is shut down");
        }
    }

    #[tokio::test]
    async fn oversized_body_is_rejected_before_sql() {
        let fake = Arc::new(Fake {
            requests: Mutex::new(Vec::new()),
            outcome: ready_ok(vec![statement("n", 1)]),
        });
        let executor: Arc<dyn SqlExecutor> = fake.clone();
        let mut state = AppState::new(executor, None);
        state.max_body_bytes = 64;
        let app = router(state);
        let body = format!(r#"{{"sql":"{}"}}"#, "A".repeat(200));
        let response = app
            .oneshot(post(QUERY, None, &body))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert!(fake.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn oversized_response_is_rejected() {
        let big = "x".repeat(4_000);
        let statement = StatementResult {
            columns: Arc::from(vec!["v".to_string()]),
            rows: vec![vec![SqlValue::Text(big.clone())]],
            meta: sample_meta(),
            success: true,
        };
        let fake = Arc::new(Fake {
            requests: Mutex::new(Vec::new()),
            outcome: ready_ok(vec![statement]),
        });
        let executor: Arc<dyn SqlExecutor> = fake.clone();
        let mut state = AppState::new(executor, None);
        state.max_response_bytes = 128;
        let app = router(state);
        let response = app
            .oneshot(post(QUERY, None, r#"{"sql":"SELECT 1"}"#))
            .await
            .expect("response");
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let text = String::from_utf8_lossy(&bytes);
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(text.contains("response exceeds the size limit"));
        assert!(!text.contains(&big));
    }

    #[tokio::test]
    async fn sql_request_timeout_returns_504() {
        let fake = Arc::new(Fake {
            requests: Mutex::new(Vec::new()),
            outcome: Outcome::Pending,
        });
        let executor: Arc<dyn SqlExecutor> = fake.clone();
        let mut state = AppState::new(executor, None);
        state.request_timeout = Some(Duration::from_millis(30));
        let app = router(state);
        let started = Instant::now();
        let (status, headers, body) =
            dispatch(app, post(QUERY, None, r#"{"sql":"SELECT 1"}"#)).await;
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
        assert_failure(&body, 7500, "sql request timed out");
        assert_eq!(
            headers
                .get("x-should-retry")
                .and_then(|value| value.to_str().ok()),
            Some("false")
        );
        assert!(started.elapsed() < Duration::from_secs(2));
    }
}
