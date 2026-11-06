//! D1 REST `/query` and `/raw` HTTP adapter.

use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
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
        }
    }
}

/// Routes `GET /health`, `POST /accounts/{account_id}/d1/database/{database_id}/query`,
/// and the matching `/raw` route.
///
/// `/health` does not enqueue. Path ids are ignored.
pub fn router(state: AppState) -> Router {
    let max_body_bytes = state.max_body_bytes;
    Router::new()
        .route("/health", get(health))
        .route(
            "/accounts/{account_id}/d1/database/{database_id}/query",
            post(query),
        )
        .route(
            "/accounts/{account_id}/d1/database/{database_id}/raw",
            post(raw),
        )
        .layer(DefaultBodyLimit::max(max_body_bytes))
        .with_state(state)
}

async fn health() -> Response {
    json_response(StatusCode::OK, HealthBody { ok: true })
}

async fn query(
    State(state): State<AppState>,
    Path((_, _)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    handle(&state, &headers, &body, ResultShape::Objects).await
}

async fn raw(
    State(state): State<AppState>,
    Path((_, _)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    handle(&state, &headers, &body, ResultShape::Raw).await
}

async fn handle(
    state: &AppState,
    headers: &HeaderMap,
    body: &[u8],
    shape: ResultShape,
) -> Response {
    if let Err(response) = authorize(state, headers) {
        return *response;
    }
    let request = match parse_request(body, shape) {
        Ok(request) => request,
        Err(response) => return *response,
    };
    match state.executor.enqueue(request) {
        EnqueueResult::Accepted(receiver) => wait_for_sql(state, receiver, shape).await,
        EnqueueResult::Full => queue_full(),
        EnqueueResult::Closed => shut_down(),
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
            Ok(Err(SqlError::ShuttingDown)) => shut_down(),
            Ok(Err(error)) => fail(StatusCode::BAD_REQUEST, ERROR_SQL, error.to_string()),
            // The worker dropped the response channel.
            Err(_recv) => shut_down(),
        }
    };
    match state.request_timeout {
        Some(limit) => match tokio::time::timeout(limit, pending).await {
            Ok(response) => response,
            Err(_elapsed) => request_timed_out(),
        },
        None => pending.await,
    }
}

fn limited_success(state: &AppState, response: &SqlResponse, shape: ResultShape) -> Response {
    let bytes = serde_json::to_vec(&success_body(response, shape)).expect("envelope is json");
    if bytes.len() > state.max_response_bytes {
        return fail(
            StatusCode::BAD_REQUEST,
            ERROR_SQL,
            "response exceeds the size limit",
        );
    }
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        )],
        bytes,
    )
        .into_response()
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
            served_by_colo: meta.served_by_colo.clone(),
            served_by_primary: meta.served_by_primary,
            served_by_region: meta.served_by_region.clone(),
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
            columns: statement.columns.clone(),
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
    use std::sync::{Arc, Mutex};

    use axum::body::Body;
    use axum::http::{header, HeaderMap, Request, StatusCode};
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
            served_by_colo: "LOC".to_string(),
            served_by_region: "WNAM".to_string(),
            served_by_primary: false,
        }
    }

    fn statement(column: &str, value: i64) -> StatementResult {
        StatementResult {
            columns: vec![column.to_string()],
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
        assert_eq!(body, json!({ "ok": true }));
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
            columns: vec!["v".to_string()],
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
        let (status, headers, body) = dispatch(app, post(QUERY, None, r#"{"sql":"SELECT 1"}"#)).await;
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
