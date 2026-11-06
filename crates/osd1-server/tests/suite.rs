//! SQL, concurrency, fuzz, and process-kill checks against the real worker.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use osd1_engine::SqliteWorker;
use osd1_http::{router, AppState};
use osd1_types::SqlExecutor;
use serde_json::{json, Value};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct Running {
    addr: SocketAddr,
    token: String,
    dir: PathBuf,
    executor: Option<Arc<dyn SqlExecutor>>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for Running {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
        self.executor.take();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl Running {
    async fn start(capacity: usize) -> Self {
        Self::open(
            capacity,
            "suite-token",
            1024 * 1024,
            Some(Duration::from_secs(30)),
        )
        .await
    }

    async fn open(
        capacity: usize,
        token: &str,
        max_body_bytes: usize,
        request_timeout: Option<Duration>,
    ) -> Self {
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("osd1-suite-{}-{n}", std::process::id()));
        let worker =
            SqliteWorker::open(dir.join("osd1.db"), capacity, "LOC").expect("open database");
        let executor: Arc<dyn SqlExecutor> = Arc::new(worker);
        let mut state = AppState::new(Arc::clone(&executor), Some(token.to_string()));
        state.max_body_bytes = max_body_bytes;
        state.request_timeout = request_timeout;
        let app = router(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let (shutdown, rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = rx.await;
                })
                .await;
        });
        let running = Self {
            addr,
            token: token.to_string(),
            dir,
            executor: Some(executor),
            shutdown: Some(shutdown),
            task: Some(task),
        };
        running.get("/health").await;
        running
    }

    async fn post(&self, sql: &str, params: Option<Value>) -> (u16, Value) {
        let body = match params {
            Some(params) => json!({"sql": sql, "params": params}).to_string(),
            None => json!({"sql": sql}).to_string(),
        };
        self.post_raw(&body).await
    }

    async fn post_raw(&self, body: &str) -> (u16, Value) {
        let (status, text) = exchange(
            self.addr,
            "POST",
            "/accounts/suite/d1/database/suite/query",
            Some(&self.token),
            Some(body),
        )
        .await;
        let parsed = serde_json::from_str(&text).unwrap_or(Value::Null);
        (status, parsed)
    }

    async fn get(&self, path: &str) -> (u16, String) {
        exchange(self.addr, "GET", path, None, None).await
    }

    async fn ok(&self, sql: &str, params: Option<Value>) -> Value {
        let (status, body) = self.post(sql, params).await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["success"], json!(true), "{body}");
        body
    }
}

async fn exchange(
    addr: SocketAddr,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: Option<&str>,
) -> (u16, String) {
    let method = method.to_string();
    let path = path.to_string();
    let token = token.map(str::to_string);
    let body = body.map(str::to_string);
    tokio::task::spawn_blocking(move || {
        exchange_blocking(addr, &method, &path, token.as_deref(), body.as_deref())
    })
    .await
    .expect("http thread")
}

fn exchange_blocking(
    addr: SocketAddr,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: Option<&str>,
) -> (u16, String) {
    let body = body.unwrap_or("");
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    );
    if method == "POST" {
        request.push_str("Content-Type: application/json\r\n");
    }
    if let Some(token) = token {
        request.push_str(&format!("Authorization: Bearer {token}\r\n"));
    }
    request.push_str("\r\n");
    request.push_str(body);
    let mut last = String::new();
    for _ in 0..30 {
        match TcpStream::connect(addr) {
            Ok(mut stream) => {
                stream
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                stream
                    .set_write_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                stream.write_all(request.as_bytes()).unwrap();
                let mut buf = Vec::new();
                stream.read_to_end(&mut buf).unwrap();
                let text = String::from_utf8_lossy(&buf).into_owned();
                let (head, resp) = text.split_once("\r\n\r\n").unwrap_or(("", &text));
                let status = head
                    .split_whitespace()
                    .nth(1)
                    .unwrap_or("0")
                    .parse()
                    .unwrap_or(0);
                return (status, resp.to_string());
            }
            Err(error) => {
                last = error.to_string();
                std::thread::sleep(Duration::from_millis(5));
            }
        }
    }
    panic!("connect {addr}: {last}");
}

fn cell<'a>(body: &'a Value, column: &str) -> &'a Value {
    &body["result"][0]["results"][0][column]
}

fn message(body: &Value) -> String {
    body["errors"][0]["message"]
        .as_str()
        .unwrap_or("")
        .to_string()
}

#[tokio::test]
async fn sql_crud_ddl_joins_indexes_and_ctes() {
    let server = Running::start(16).await;
    server
        .ok(
            "CREATE TABLE authors (id INTEGER PRIMARY KEY, name TEXT);
             CREATE TABLE books (id INTEGER PRIMARY KEY, author_id INTEGER, title TEXT)",
            None,
        )
        .await;
    server
        .ok(
            "INSERT INTO authors (name) VALUES (?)",
            Some(json!(["ada"])),
        )
        .await;
    server
        .ok(
            "INSERT INTO books (author_id, title) VALUES (?, ?)",
            Some(json!([1, "notes"])),
        )
        .await;
    let updated = server
        .ok(
            "UPDATE books SET title = ? WHERE id = 1",
            Some(json!(["revised"])),
        )
        .await;
    assert_eq!(updated["result"][0]["meta"]["changes"], json!(1));
    let joined = server
        .ok(
            "SELECT authors.name AS name, books.title AS title
             FROM authors JOIN books ON books.author_id = authors.id",
            None,
        )
        .await;
    assert_eq!(cell(&joined, "name"), &json!("ada"));
    assert_eq!(cell(&joined, "title"), &json!("revised"));
    server
        .ok("CREATE INDEX books_author ON books (author_id)", None)
        .await;
    let plan = server
        .ok(
            "EXPLAIN QUERY PLAN SELECT title FROM books WHERE author_id = 1",
            None,
        )
        .await;
    assert!(
        plan.to_string().contains("books_author"),
        "index not used: {plan}"
    );
    let cte = server
        .ok(
            "WITH nums(n) AS (
                SELECT 1 UNION ALL SELECT n + 1 FROM nums WHERE n < 3
             )
             SELECT n FROM nums ORDER BY n",
            None,
        )
        .await;
    assert_eq!(
        cte["result"][0]["results"],
        json!([{"n": 1}, {"n": 2}, {"n": 3}])
    );
    server.ok("DELETE FROM books WHERE id = 1", None).await;
    let left = server.ok("SELECT COUNT(*) AS n FROM books", None).await;
    assert_eq!(cell(&left, "n"), &json!(0));
}

#[tokio::test]
async fn sql_types_cover_null_integer_float_text_and_reject_blobs() {
    let server = Running::start(4).await;
    server.ok("CREATE TABLE kinds (i, r, t, z)", None).await;
    server
        .ok(
            "INSERT INTO kinds VALUES (?, ?, ?, ?)",
            Some(json!([7, 1.25, "plain", null])),
        )
        .await;
    let row = server.ok("SELECT i, r, t, z FROM kinds", None).await;
    assert_eq!(cell(&row, "i"), &json!(7));
    assert_eq!(cell(&row, "r").as_f64(), Some(1.25));
    assert_eq!(cell(&row, "t"), &json!("plain"));
    assert!(cell(&row, "z").is_null());
    let (status, body) = server.post("SELECT x'6869'", None).await;
    assert_eq!(status, 400, "{body}");
    assert!(message(&body).contains("blob"), "{body}");
}

#[tokio::test]
async fn transactions_commit_rollback_and_constraint_failures() {
    let server = Running::start(4).await;
    server
        .ok(
            "CREATE TABLE parent (id INTEGER PRIMARY KEY);
             CREATE TABLE child (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER REFERENCES parent(id)
             );
             CREATE TABLE uniq (n INTEGER UNIQUE)",
            None,
        )
        .await;
    server.ok("INSERT INTO uniq (n) VALUES (1)", None).await;
    let (status, body) = server
        .post(
            "INSERT INTO uniq (n) VALUES (2); INSERT INTO uniq (n) VALUES (1)",
            None,
        )
        .await;
    assert_eq!(status, 400, "{body}");
    let left = server.ok("SELECT n FROM uniq ORDER BY n", None).await;
    assert_eq!(left["result"][0]["results"], json!([{"n": 1}]));

    let (status, body) = server
        .post("INSERT INTO child (parent_id) VALUES (9)", None)
        .await;
    assert_eq!(status, 400, "{body}");
    let children = server.ok("SELECT COUNT(*) AS n FROM child", None).await;
    assert_eq!(cell(&children, "n"), &json!(0));

    server.ok("INSERT INTO parent (id) VALUES (9)", None).await;
    server
        .ok("INSERT INTO child (parent_id) VALUES (9)", None)
        .await;
    let committed = server.ok("SELECT parent_id FROM child", None).await;
    assert_eq!(cell(&committed, "parent_id"), &json!(9));

    let (status, body) = server.post("BEGIN", None).await;
    assert_eq!(status, 400, "{body}");
    assert!(message(&body).contains("transaction"), "{body}");
    let (status, body) = server.post("COMMIT", None).await;
    assert_eq!(status, 400, "{body}");
    let (status, body) = server.post("ROLLBACK", None).await;
    assert_eq!(status, 400, "{body}");
}

#[tokio::test]
async fn simultaneous_clients_insert_every_row() {
    for clients in [10_usize, 100, 500] {
        let server = Running::start(clients + 8).await;
        server
            .ok("CREATE TABLE hits (id INTEGER PRIMARY KEY)", None)
            .await;
        let addr = server.addr;
        let token = server.token.clone();
        let statuses = tokio::task::spawn_blocking(move || {
            std::thread::scope(|scope| {
                let mut joins = Vec::with_capacity(clients);
                for id in 0..clients {
                    let token = token.clone();
                    joins.push(scope.spawn(move || {
                        let body = format!(r#"{{"sql":"INSERT INTO hits (id) VALUES ({id})"}}"#);
                        let (status, _) = exchange_blocking(
                            addr,
                            "POST",
                            "/accounts/suite/d1/database/suite/query",
                            Some(&token),
                            Some(&body),
                        );
                        status
                    }));
                }
                joins
                    .into_iter()
                    .map(|join| join.join().expect("client"))
                    .collect::<Vec<_>>()
            })
        })
        .await
        .expect("clients");
        assert!(
            statuses.iter().all(|status| *status == 200),
            "{clients} clients: {statuses:?}"
        );
        let count = server.ok("SELECT COUNT(*) AS n FROM hits", None).await;
        assert_eq!(cell(&count, "n"), &json!(clients), "{clients}");
    }
}

#[tokio::test]
async fn a_full_queue_rejects_overflow_and_keeps_accepted_rows() {
    let server = Running::start(1).await;
    server
        .ok("CREATE TABLE hits (id INTEGER PRIMARY KEY)", None)
        .await;
    let addr = server.addr;
    let token = server.token.clone();
    let statuses = tokio::task::spawn_blocking(move || {
        std::thread::scope(|scope| {
            let mut joins = Vec::new();
            for id in 0..80 {
                let token = token.clone();
                joins.push(scope.spawn(move || {
                    let body = format!(r#"{{"sql":"INSERT INTO hits (id) VALUES ({id})"}}"#);
                    let (status, _) = exchange_blocking(
                        addr,
                        "POST",
                        "/accounts/suite/d1/database/suite/query",
                        Some(&token),
                        Some(&body),
                    );
                    status
                }));
            }
            joins
                .into_iter()
                .map(|join| join.join().expect("client"))
                .collect::<Vec<_>>()
        })
    })
    .await
    .expect("clients");
    let accepted = statuses.iter().filter(|status| **status == 200).count();
    let rejected = statuses.iter().filter(|status| **status == 429).count();
    assert!(rejected > 0, "expected a full queue, got {statuses:?}");
    assert_eq!(accepted + rejected, statuses.len(), "{statuses:?}");
    let count = server.ok("SELECT COUNT(*) AS n FROM hits", None).await;
    assert_eq!(cell(&count, "n").as_u64(), Some(accepted as u64));
}

#[tokio::test]
async fn bearer_token_is_required_when_configured() {
    let server = Running::start(4).await;
    let (status, _) = exchange(
        server.addr,
        "POST",
        "/accounts/suite/d1/database/suite/query",
        None,
        Some(r#"{"sql":"SELECT 1"}"#),
    )
    .await;
    assert_eq!(status, 401);
    let (status, _) = exchange(
        server.addr,
        "POST",
        "/accounts/suite/d1/database/suite/query",
        Some("wrong-token"),
        Some(r#"{"sql":"SELECT 1"}"#),
    )
    .await;
    assert_eq!(status, 401);
    let (status, body) = server.post("SELECT 1 AS n", None).await;
    assert_eq!(status, 200, "{body}");
}

#[tokio::test]
async fn authorizer_blocks_extension_filesystem_and_attach() {
    let server = Running::start(4).await;
    let spill = server.dir.join("spill.txt");
    let other = server.dir.join("other.db");
    let statements = [
        "SELECT load_extension('nope')".to_string(),
        "SELECT readfile('/etc/hosts')".to_string(),
        format!("SELECT writefile('{}', 'nope')", spill.display()),
        format!("ATTACH DATABASE '{}' AS other", other.display()),
    ];
    for sql in statements {
        let (status, body) = server.post(&sql, None).await;
        assert_eq!(status, 400, "{sql}: {body}");
        assert_eq!(body["errors"][0]["code"], json!(7500), "{body}");
    }
    assert!(!spill.exists());
    assert!(!other.exists());
}

#[tokio::test]
async fn malformed_input_stays_a_client_error() {
    let server = Running::open(4, "suite-token", 128, Some(Duration::from_secs(5))).await;
    let mut samples = vec![
        String::new(),
        "null".to_string(),
        "[]".to_string(),
        "{".to_string(),
        r#"{"sql":1}"#.to_string(),
        r#"{"sql":"SELECT 1","params":{}}"#.to_string(),
        r#"{"sql":"SELECT 1","params":[true]}"#.to_string(),
        r#"{"sql":"SELECT 1","params":[{"a":1}]}"#.to_string(),
        "\0\0".to_string(),
        "x".repeat(80),
        "y".repeat(400),
    ];
    let mut state = 0x1234_5678_u64;
    for _ in 0..40 {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let len = (state % 90) as usize;
        let byte = (state >> 8) as u8;
        samples.push(vec![byte; len].into_iter().map(|b| b as char).collect());
    }
    for sample in samples {
        let (status, _) = server.post_raw(&sample).await;
        assert!(
            status == 400 || status == 413,
            "status {status} for {sample:?}"
        );
    }
    let (status, _) = server.get("/health").await;
    assert_eq!(status, 200);
}

#[test]
fn process_kill_keeps_committed_rows_and_hides_the_token() {
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "osd1-kill-{}-{n}/process-kill-token",
        std::process::id()
    ));
    let db = dir.join("osd1.db");
    let log = std::env::temp_dir().join(format!("osd1-kill-{n}.log"));
    let port = free_port();
    let token = "process-kill-token";
    let stderr = std::fs::File::create(&log).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_osd1"))
        .env("OSD1_DB_PATH", &db)
        .env("OSD1_HTTP_ADDR", format!("127.0.0.1:{port}"))
        .env("OSD1_API_TOKEN", token)
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr))
        .spawn()
        .expect("spawn osd1");
    wait_health(port);
    let addr = format!("127.0.0.1:{port}").parse().unwrap();
    let (status, _) = exchange_blocking(
        addr,
        "POST",
        "/accounts/kill/d1/database/kill/query",
        Some(token),
        Some(
            r#"{"sql":"CREATE TABLE kept (note TEXT); INSERT INTO kept (note) VALUES ('still-here')"}"#,
        ),
    );
    assert_eq!(status, 200);
    let logged = std::fs::read_to_string(&log).unwrap_or_default();
    assert!(
        !logged.contains(token),
        "startup log contains the bearer token: {logged}"
    );
    assert!(logged.contains("[redacted]"), "{logged}");
    child.kill().expect("kill");
    let _ = child.wait();

    let mut restarted = Command::new(env!("CARGO_BIN_EXE_osd1"))
        .env("OSD1_DB_PATH", &db)
        .env("OSD1_HTTP_ADDR", format!("127.0.0.1:{port}"))
        .env("OSD1_API_TOKEN", token)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("restart osd1");
    wait_health(port);
    let (status, body) = exchange_blocking(
        addr,
        "POST",
        "/accounts/kill/d1/database/kill/query",
        Some(token),
        Some(r#"{"sql":"SELECT note FROM kept"}"#),
    );
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("still-here"), "{body}");
    restarted.kill().expect("stop");
    let _ = restarted.wait();
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
    let _ = std::fs::remove_file(log);
}

fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn wait_health(port: u16) {
    let addr = format!("127.0.0.1:{port}").parse().unwrap();
    let started = std::time::Instant::now();
    while started.elapsed() < Duration::from_secs(10) {
        if let Ok((status, body)) =
            std::panic::catch_unwind(|| exchange_blocking(addr, "GET", "/health", None, None))
        {
            if status == 200 && body.contains("true") {
                return;
            }
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("osd1 did not become healthy on {port}");
}
