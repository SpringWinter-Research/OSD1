//! One OS thread, one SQLite connection, one bounded FIFO queue.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvError, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Instant;

use osd1_types::{
    EnqueueResult, SqlError, SqlExecutor, SqlRequest, SqlResponse, SqlValue,
    StatementMeta, StatementResult, SERVED_BY_REGION,
};
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::types::{Value as SqliteValue, ValueRef};
use rusqlite::{params_from_iter, Batch, Connection};

/// Test gate. The worker signals `parked` after it dequeues a job and before it runs SQL,
/// then blocks on `release`.
pub(crate) struct PauseGate {
    pub parked: mpsc::Sender<()>,
    pub release: Receiver<()>,
}

struct Job {
    request: SqlRequest,
    reply: tokio::sync::oneshot::Sender<Result<SqlResponse, SqlError>>,
}

pub struct SqliteWorker {
    tx: Option<SyncSender<Job>>,
    handle: Option<JoinHandle<()>>,
}

impl SqliteWorker {
    pub fn open(
        path: impl AsRef<Path>,
        capacity: usize,
        served_by_colo: impl Into<String>,
    ) -> Result<Self, rusqlite::Error> {
        Self::start(path, capacity, served_by_colo, None)
    }

    #[cfg(test)]
    pub(crate) fn open_with_pause(
        path: impl AsRef<Path>,
        capacity: usize,
        served_by_colo: impl Into<String>,
        pause: PauseGate,
    ) -> Result<Self, rusqlite::Error> {
        Self::start(path, capacity, served_by_colo, Some(pause))
    }

    fn start(
        path: impl AsRef<Path>,
        capacity: usize,
        served_by_colo: impl Into<String>,
        pause: Option<PauseGate>,
    ) -> Result<Self, rusqlite::Error> {
        let path = path.as_ref().to_path_buf();
        ensure_private_parent(&path)?;
        let capacity = capacity.max(1);
        let served_by_colo = served_by_colo.into();
        let (tx, rx) = mpsc::sync_channel(capacity);
        let (ready_tx, ready_rx) = mpsc::channel();
        let handle = thread::Builder::new()
            .name("osd1-sql".to_string())
            .spawn(move || worker_main(path, rx, pause, served_by_colo, ready_tx))
            .map_err(open_error)?;
        match ready_rx.recv() {
            Ok(Ok(())) => Ok(SqliteWorker {
                tx: Some(tx),
                handle: Some(handle),
            }),
            Ok(Err(error)) => {
                let _ = handle.join();
                Err(error)
            }
            Err(_) => {
                let _ = handle.join();
                Err(open_error(std::io::Error::other(
                    "sql worker stopped before the database opened",
                )))
            }
        }
    }
}

impl SqlExecutor for SqliteWorker {
    fn enqueue(&self, request: SqlRequest) -> EnqueueResult {
        let Some(tx) = &self.tx else {
            return EnqueueResult::Closed;
        };
        let (reply, receiver) = tokio::sync::oneshot::channel();
        match tx.try_send(Job { request, reply }) {
            Ok(()) => EnqueueResult::Accepted(receiver),
            Err(TrySendError::Full(_)) => EnqueueResult::Full,
            Err(TrySendError::Disconnected(_)) => EnqueueResult::Closed,
        }
    }
}

impl Drop for SqliteWorker {
    fn drop(&mut self) {
        self.tx.take();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// Create the parent directory at mode 0700 when it does not already exist.
/// An existing directory is left alone so a path under `/tmp` is not chmod'd.
fn ensure_private_parent(path: &Path) -> Result<(), rusqlite::Error> {
    let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) else {
        return Ok(());
    };
    if parent.exists() {
        return Ok(());
    }
    create_private_dir(parent).map_err(open_error)
}

#[cfg(unix)]
fn create_private_dir(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(path)?;
    // DirBuilder still applies the process umask. Set the mode again.
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
}

#[cfg(not(unix))]
fn create_private_dir(path: &Path) -> std::io::Result<()> {
    fs::create_dir_all(path)
}

fn restrict_database_files(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        for suffix in ["", "-wal", "-shm"] {
            let candidate = if suffix.is_empty() {
                path.to_path_buf()
            } else {
                PathBuf::from(format!("{}{suffix}", path.display()))
            };
            if candidate.exists() {
                let _ = fs::set_permissions(&candidate, fs::Permissions::from_mode(0o600));
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
}

fn open_error(error: std::io::Error) -> rusqlite::Error {
    rusqlite::Error::SqliteFailure(
        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CANTOPEN),
        Some(error.to_string()),
    )
}

fn worker_main(
    path: PathBuf,
    rx: Receiver<Job>,
    pause: Option<PauseGate>,
    served_by_colo: String,
    ready: mpsc::Sender<Result<(), rusqlite::Error>>,
) {
    let mut conn = match Connection::open(&path) {
        Ok(conn) => conn,
        Err(error) => {
            let _ = ready.send(Err(error));
            return;
        }
    };
    restrict_database_files(&path);
    if let Err(error) = prepare_connection(&conn) {
        let _ = ready.send(Err(error));
        return;
    }
    restrict_database_files(&path);
    let internal = Arc::new(AtomicBool::new(false));
    let schema_changed = Arc::new(AtomicBool::new(false));
    let deny_reason = Arc::new(Mutex::new(String::new()));
    if let Err(error) = install_authorizer(
        &conn,
        Arc::clone(&internal),
        Arc::clone(&schema_changed),
        Arc::clone(&deny_reason),
    ) {
        let _ = ready.send(Err(error));
        return;
    }
    if ready.send(Ok(())).is_err() {
        return;
    }

    let mut poisoned = false;
    loop {
        let job = match rx.recv() {
            Ok(job) => job,
            Err(RecvError) => break,
        };
        if let Some(gate) = &pause {
            if gate.parked.send(()).is_err() || gate.release.recv().is_err() {
                break;
            }
        }
        let result = if poisoned {
            Err(SqlError::TransactionLeftOpen)
        } else {
            execute_request(
                &mut conn,
                &path,
                &served_by_colo,
                &internal,
                &schema_changed,
                &deny_reason,
                job.request,
            )
        };
        if matches!(result, Err(SqlError::TransactionLeftOpen)) {
            poisoned = true;
        }
        let _ = job.reply.send(result);
    }
}

fn prepare_connection(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(
        "
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=FULL;
        PRAGMA foreign_keys=ON;
        PRAGMA busy_timeout=0;
        ",
    )
}

fn install_authorizer(
    conn: &Connection,
    internal: Arc<AtomicBool>,
    schema_changed: Arc<AtomicBool>,
    deny_reason: Arc<Mutex<String>>,
) -> Result<(), rusqlite::Error> {
    conn.authorizer(Some(move |context: AuthContext<'_>| {
        if is_schema_action(&context.action) {
            schema_changed.store(true, Ordering::SeqCst);
        }
        match deny_context(&context, internal.load(Ordering::SeqCst)) {
            None => Authorization::Allow,
            Some(reason) => {
                if let Ok(mut slot) = deny_reason.lock() {
                    *slot = reason.to_string();
                }
                Authorization::Deny
            }
        }
    }));
    Ok(())
}

fn is_schema_action(action: &AuthAction<'_>) -> bool {
    matches!(
        action,
        AuthAction::CreateIndex { .. }
            | AuthAction::CreateTable { .. }
            | AuthAction::CreateTempIndex { .. }
            | AuthAction::CreateTempTable { .. }
            | AuthAction::CreateTempTrigger { .. }
            | AuthAction::CreateTempView { .. }
            | AuthAction::CreateTrigger { .. }
            | AuthAction::CreateView { .. }
            | AuthAction::DropIndex { .. }
            | AuthAction::DropTable { .. }
            | AuthAction::DropTempIndex { .. }
            | AuthAction::DropTempTable { .. }
            | AuthAction::DropTempTrigger { .. }
            | AuthAction::DropTempView { .. }
            | AuthAction::DropTrigger { .. }
            | AuthAction::DropView { .. }
            | AuthAction::AlterTable { .. }
    )
}

fn deny_context(context: &AuthContext<'_>, internal: bool) -> Option<&'static str> {
    if let Some(database) = context
        .database_name
        .filter(|name| !name.is_empty())
    {
        if !database.eq_ignore_ascii_case("main") && !database.eq_ignore_ascii_case("temp") {
            return Some("only the main database is allowed");
        }
    }
    let action = &context.action;
    match action {
        AuthAction::Attach { .. } => Some("ATTACH is not allowed"),
        AuthAction::Detach { .. } => Some("DETACH is not allowed"),
        AuthAction::Transaction { .. } | AuthAction::Savepoint { .. } if !internal => {
            Some("client transactions are not allowed")
        }
        AuthAction::Pragma { pragma_name, .. }
            if pragma_name.eq_ignore_ascii_case("writable_schema") =>
        {
            Some("writable_schema is not allowed")
        }
        AuthAction::Function { function_name } if is_denied_function(function_name) => {
            Some("function is not allowed")
        }
        AuthAction::AlterTable { database_name, .. }
            if !database_name.eq_ignore_ascii_case("main")
                && !database_name.eq_ignore_ascii_case("temp") =>
        {
            Some("only the main database is allowed")
        }
        _ => None,
    }
}

fn is_denied_function(name: &str) -> bool {
    name.eq_ignore_ascii_case("load_extension")
        || name.eq_ignore_ascii_case("readfile")
        || name.eq_ignore_ascii_case("writefile")
}

fn execute_request(
    conn: &mut Connection,
    path: &Path,
    served_by_colo: &str,
    internal: &AtomicBool,
    schema_changed: &AtomicBool,
    deny_reason: &Mutex<String>,
    request: SqlRequest,
) -> Result<SqlResponse, SqlError> {
    if let Ok(mut reason) = deny_reason.lock() {
        reason.clear();
    }
    allow_internal(internal, || conn.execute_batch("BEGIN IMMEDIATE"))
        .map_err(|error| map_sqlite_error(error, deny_reason))?;

    let statements = match run_batch(
        conn,
        &request,
        served_by_colo,
        schema_changed,
        deny_reason,
    ) {
        Ok(statements) => statements,
        Err(error) => {
            rollback(conn, internal);
            return Err(error);
        }
    };

    if let Err(error) = allow_internal(internal, || conn.execute_batch("COMMIT"))
        .map_err(|error| map_sqlite_error(error, deny_reason))
    {
        rollback(conn, internal);
        return Err(error);
    }
    if !conn.is_autocommit() {
        rollback(conn, internal);
        if !conn.is_autocommit() {
            return Err(SqlError::TransactionLeftOpen);
        }
    }

    let size_after = fs::metadata(path).map(|meta| meta.len()).unwrap_or(0);
    let mut statements = statements;
    for statement in &mut statements {
        statement.meta.size_after = size_after;
    }
    Ok(SqlResponse { statements })
}

fn run_batch(
    conn: &Connection,
    request: &SqlRequest,
    served_by_colo: &str,
    schema_changed: &AtomicBool,
    deny_reason: &Mutex<String>,
) -> Result<Vec<StatementResult>, SqlError> {
    let mut batch = Batch::new(conn, &request.sql);
    let mut statements = Vec::new();
    loop {
        schema_changed.store(false, Ordering::SeqCst);
        let total_before = conn.total_changes();
        let mut stmt = match batch.next() {
            Ok(Some(stmt)) => stmt,
            Ok(None) => break,
            Err(error) => return Err(map_sqlite_error(error, deny_reason)),
        };
        let started = Instant::now();
        let bound = bind_params(&stmt, &request.params)?;
        let columns: Vec<String> = (0..stmt.column_count())
            .map(|index| stmt.column_name(index).unwrap_or("").to_string())
            .collect();
        let mut rows_iter = stmt
            .query(params_from_iter(bound))
            .map_err(|error| map_sqlite_error(error, deny_reason))?;
        let mut rows = Vec::new();
        loop {
            let next = rows_iter
                .next()
                .map_err(|error| map_sqlite_error(error, deny_reason))?;
            let Some(row) = next else { break };
            let mut values = Vec::with_capacity(columns.len());
            for index in 0..columns.len() {
                let value = row
                    .get_ref(index)
                    .map_err(|error| map_sqlite_error(error, deny_reason))?;
                values.push(value_from_ref(value)?);
            }
            rows.push(values);
        }
        drop(rows_iter);
        drop(stmt);
        let rows_read = rows.len() as u64;
        let changes = conn.total_changes().saturating_sub(total_before);
        let duration_ms = started.elapsed().as_secs_f64() * 1000.0;
        statements.push(StatementResult {
            columns,
            rows,
            success: true,
            meta: StatementMeta {
                changed_db: changes > 0 || schema_changed.load(Ordering::SeqCst),
                changes,
                duration_ms,
                last_row_id: conn.last_insert_rowid(),
                rows_read,
                rows_written: changes,
                size_after: 0,
                served_by_colo: served_by_colo.to_string(),
                served_by_region: SERVED_BY_REGION.to_string(),
                served_by_primary: true,
            },
        });
    }
    if statements.is_empty() {
        return Err(SqlError::Sqlite {
            code: 1,
            message: "empty sql".to_string(),
        });
    }
    Ok(statements)
}

fn bind_params(stmt: &rusqlite::Statement<'_>, params: &[SqlValue]) -> Result<Vec<SqliteValue>, SqlError> {
    let count = stmt.parameter_count();
    if count == 0 {
        return Ok(Vec::new());
    }
    if count != params.len() {
        return Err(SqlError::Sqlite {
            code: 1,
            message: "parameter count mismatch".to_string(),
        });
    }
    Ok(params.iter().map(sqlite_value).collect())
}

fn sqlite_value(value: &SqlValue) -> SqliteValue {
    match value {
        SqlValue::Null => SqliteValue::Null,
        SqlValue::Integer(value) => SqliteValue::Integer(*value),
        SqlValue::Real(value) => SqliteValue::Real(*value),
        SqlValue::Text(value) => SqliteValue::Text(value.clone()),
    }
}

fn value_from_ref(value: ValueRef<'_>) -> Result<SqlValue, SqlError> {
    match value {
        ValueRef::Null => Ok(SqlValue::Null),
        ValueRef::Integer(value) => Ok(SqlValue::Integer(value)),
        ValueRef::Real(value) => Ok(SqlValue::Real(value)),
        ValueRef::Text(bytes) => Ok(SqlValue::Text(String::from_utf8_lossy(bytes).into_owned())),
        ValueRef::Blob(_) => Err(SqlError::Sqlite {
            code: 1,
            message: "blob results are not supported".to_string(),
        }),
    }
}

fn rollback(conn: &Connection, internal: &AtomicBool) {
    let _ = allow_internal(internal, || conn.execute_batch("ROLLBACK"));
}

fn allow_internal<T>(internal: &AtomicBool, f: impl FnOnce() -> T) -> T {
    internal.store(true, Ordering::SeqCst);
    struct Clear<'a>(&'a AtomicBool);
    impl Drop for Clear<'_> {
        fn drop(&mut self) {
            self.0.store(false, Ordering::SeqCst);
        }
    }
    let _clear = Clear(internal);
    f()
}

fn map_sqlite_error(error: rusqlite::Error, deny_reason: &Mutex<String>) -> SqlError {
    let (code, message) = match &error {
        rusqlite::Error::SqliteFailure(ffi, detail) => (
            ffi.extended_code,
            detail.clone().unwrap_or_else(|| error.to_string()),
        ),
        other => (1, other.to_string()),
    };
    if code == 23 || code & 0xff == 23 {
        let reason = deny_reason
            .lock()
            .map(|slot| slot.clone())
            .unwrap_or_default();
        return SqlError::AuthorizerDenied {
            message: if reason.is_empty() {
                "not authorized".to_string()
            } else {
                reason
            },
        };
    }
    SqlError::Sqlite { code, message }
}

#[cfg(test)]
mod tests {
    use super::*;
    use osd1_types::{ResultShape, SqlError};
    use std::sync::atomic::AtomicU64;

    fn temp_db(name: &str) -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "osd1-{}-{}-{name}.db",
            std::process::id(),
            n
        ))
    }

    fn run(worker: &SqliteWorker, sql: &str, params: Vec<SqlValue>) -> Result<SqlResponse, SqlError> {
        match worker.enqueue(SqlRequest {
            sql: sql.to_string(),
            params,
            shape: ResultShape::Objects,
        }) {
            EnqueueResult::Accepted(receiver) => receiver.blocking_recv().expect("worker reply"),
            EnqueueResult::Full => panic!("queue full"),
            EnqueueResult::Closed => panic!("worker closed"),
        }
    }

    fn release_job(parked: &Receiver<()>, release: &mpsc::Sender<()>) {
        parked.recv().expect("worker parked");
        release.send(()).expect("release worker");
    }

    #[test]
    fn select_one_returns_an_integer_row() {
        let path = temp_db("select1");
        let worker = SqliteWorker::open(&path, 4, "LOC").unwrap();
        let response = run(&worker, "SELECT 1 AS n", vec![]).unwrap();
        assert_eq!(response.statements.len(), 1);
        assert_eq!(response.statements[0].columns, vec!["n".to_string()]);
        assert_eq!(response.statements[0].rows, vec![vec![SqlValue::Integer(1)]]);
        assert_eq!(response.statements[0].meta.rows_read, 1);
        assert!(response.statements[0].meta.served_by_primary);
        assert_eq!(response.statements[0].meta.served_by_region, "WNAM");
    }

    #[test]
    fn parameters_are_bound_not_concatenated() {
        let path = temp_db("bind");
        let worker = SqliteWorker::open(&path, 4, "LOC").unwrap();
        let response = run(
            &worker,
            "SELECT ? AS v",
            vec![SqlValue::Text("1 OR 1".to_string())],
        )
        .unwrap();
        assert_eq!(
            response.statements[0].rows,
            vec![vec![SqlValue::Text("1 OR 1".to_string())]]
        );
    }

    #[test]
    fn requests_run_fifo_and_commits_are_visible() {
        let path = temp_db("fifo");
        let (parked_tx, parked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let worker = SqliteWorker::open_with_pause(
            &path,
            4,
            "LOC",
            PauseGate {
                parked: parked_tx,
                release: release_rx,
            },
        )
        .unwrap();
        run_paused(
            &worker,
            &parked_rx,
            &release_tx,
            "CREATE TABLE t(n TEXT)",
            vec![],
        )
        .unwrap();
        let first = enqueue(
            &worker,
            "INSERT INTO t VALUES (?)",
            vec![SqlValue::Text("first".into())],
        );
        release_job(&parked_rx, &release_tx);
        let second = enqueue(
            &worker,
            "INSERT INTO t VALUES (?)",
            vec![SqlValue::Text("second".into())],
        );
        // The first job was released. Wait until the second is parked, which means the first finished.
        release_job(&parked_rx, &release_tx);
        accept(first).blocking_recv().unwrap().unwrap();
        accept(second).blocking_recv().unwrap().unwrap();
        let response = run_paused(&worker, &parked_rx, &release_tx, "SELECT n FROM t ORDER BY rowid", vec![]).unwrap();
        assert_eq!(
            response.statements[0].rows,
            vec![
                vec![SqlValue::Text("first".into())],
                vec![SqlValue::Text("second".into())],
            ]
        );
    }

    #[test]
    fn a_full_queue_does_not_wait() {
        let path = temp_db("full");
        let (parked_tx, parked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let worker = SqliteWorker::open_with_pause(
            &path,
            1,
            "LOC",
            PauseGate {
                parked: parked_tx,
                release: release_rx,
            },
        )
        .unwrap();
        let _first = enqueue(&worker, "SELECT 1", vec![]);
        parked_rx.recv().unwrap();
        let second = enqueue(&worker, "SELECT 2", vec![]);
        assert!(matches!(second_shape(&worker), EnqueueResult::Full));
        release_tx.send(()).unwrap();
        // Let the queued job park, then release it so Drop can join.
        parked_rx.recv().unwrap();
        release_tx.send(()).unwrap();
        accept(second).blocking_recv().unwrap().unwrap();
    }

    #[test]
    fn attach_is_denied_and_creates_no_file() {
        let path = temp_db("attach");
        let other = temp_db("other");
        let worker = SqliteWorker::open(&path, 4, "LOC").unwrap();
        let error = run(
            &worker,
            &format!("ATTACH DATABASE '{}' AS other", other.display()),
            vec![],
        )
        .unwrap_err();
        assert!(matches!(error, SqlError::AuthorizerDenied { .. }), "{error:?}");
        assert!(!other.exists());
    }

    #[test]
    fn begin_is_denied() {
        let path = temp_db("begin");
        let worker = SqliteWorker::open(&path, 4, "LOC").unwrap();
        let error = run(&worker, "BEGIN", vec![]).unwrap_err();
        assert!(
            matches!(error, SqlError::AuthorizerDenied { ref message } if message.contains("transaction")),
            "{error:?}"
        );
    }

    #[test]
    fn a_failed_statement_rolls_back_the_request() {
        let path = temp_db("rollback");
        let worker = SqliteWorker::open(&path, 4, "LOC").unwrap();
        run(&worker, "CREATE TABLE t(n INTEGER)", vec![]).unwrap();
        run(&worker, "INSERT INTO t VALUES (1)", vec![]).unwrap();
        let error = run(
            &worker,
            "INSERT INTO t VALUES (2); SELECT * FROM missing",
            vec![],
        )
        .unwrap_err();
        assert!(matches!(error, SqlError::Sqlite { .. }), "{error:?}");
        let response = run(&worker, "SELECT n FROM t", vec![]).unwrap();
        assert_eq!(response.statements[0].rows, vec![vec![SqlValue::Integer(1)]]);
    }

    #[test]
    fn writable_schema_is_denied() {
        let path = temp_db("pragma");
        let worker = SqliteWorker::open(&path, 4, "LOC").unwrap();
        let error = run(&worker, "PRAGMA writable_schema=ON", vec![]).unwrap_err();
        assert!(matches!(error, SqlError::AuthorizerDenied { .. }), "{error:?}");
    }

    #[test]
    fn filesystem_functions_and_detach_are_denied() {
        let spilled = temp_db("spill");
        let cases = [
            "SELECT load_extension('nope')",
            "SELECT readfile('/etc/hosts')",
            &format!("SELECT writefile('{}', 'nope')", spilled.display()),
            "DETACH DATABASE main",
        ];
        for sql in cases {
            let path = temp_db("fs");
            let worker = SqliteWorker::open(&path, 1, "LOC").unwrap();
            let error = run(&worker, sql, vec![]).unwrap_err();
            let denied = match &error {
                SqlError::AuthorizerDenied { .. } => true,
                // Bundled SQLite rejects load_extension before the authorizer, with
                // "not authorized to use function".
                SqlError::Sqlite { message, .. } => {
                    let message = message.to_ascii_lowercase();
                    // load_extension is compiled out ("not authorized"). readfile and
                    // writefile are not part of this SQLite build ("no such function").
                    message.contains("not authorized") || message.contains("no such function")
                }
                _ => false,
            };
            assert!(denied, "{sql}: {error:?}");
        }
        assert!(!spilled.exists());
    }

    #[test]
    fn dropping_a_waiter_does_not_block_the_next_request() {
        let path = temp_db("cancel");
        let (parked_tx, parked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let worker = SqliteWorker::open_with_pause(
            &path,
            4,
            "LOC",
            PauseGate {
                parked: parked_tx,
                release: release_rx,
            },
        )
        .unwrap();
        run_paused(
            &worker,
            &parked_rx,
            &release_tx,
            "CREATE TABLE t(n TEXT)",
            vec![],
        )
        .unwrap();
        let queued = enqueue(
            &worker,
            "INSERT INTO t VALUES (?)",
            vec![SqlValue::Text("kept".into())],
        );
        // The client is gone. The worker still owns the only connection, so the
        // statement runs to commit instead of leaving a transaction open.
        drop(queued);
        release_job(&parked_rx, &release_tx);
        let next = enqueue(&worker, "SELECT n FROM t", vec![]);
        release_job(&parked_rx, &release_tx);
        let response = accept(next).blocking_recv().unwrap().unwrap();
        assert_eq!(
            response.statements[0].rows,
            vec![vec![SqlValue::Text("kept".into())]]
        );
    }

    #[test]
    fn new_database_directory_and_file_are_owner_only() {
        let n = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir()
            .join(format!("osd1-priv-{}-{n}", std::process::id()))
            .join("osd1.db");
        let worker = SqliteWorker::open(&path, 1, "LOC").unwrap();
        run(&worker, "SELECT 1", vec![]).unwrap();
        drop(worker);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let dir_mode = fs::metadata(path.parent().unwrap())
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            let file_mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(dir_mode, 0o700, "directory mode {dir_mode:o}");
            assert_eq!(file_mode, 0o600, "database mode {file_mode:o}");
            for suffix in ["-wal", "-shm"] {
                let extra = PathBuf::from(format!("{}{suffix}", path.display()));
                if extra.exists() {
                    let mode = fs::metadata(&extra).unwrap().permissions().mode() & 0o777;
                    assert_eq!(mode, 0o600, "{} mode {mode:o}", extra.display());
                }
            }
        }
        let _ = fs::remove_dir_all(path.parent().unwrap());
    }

    fn enqueue(worker: &SqliteWorker, sql: &str, params: Vec<SqlValue>) -> EnqueueResult {
        worker.enqueue(SqlRequest {
            sql: sql.to_string(),
            params,
            shape: ResultShape::Objects,
        })
    }

    fn accept(
    queued: EnqueueResult,
) -> tokio::sync::oneshot::Receiver<Result<SqlResponse, SqlError>> {
    match queued {
        EnqueueResult::Accepted(receiver) => receiver,
        EnqueueResult::Full => panic!("queue full"),
        EnqueueResult::Closed => panic!("worker closed"),
    }
}

fn second_shape(worker: &SqliteWorker) -> EnqueueResult {
        enqueue(worker, "SELECT 3", vec![])
    }

    fn run_paused(
        worker: &SqliteWorker,
        parked: &Receiver<()>,
        release: &mpsc::Sender<()>,
        sql: &str,
        params: Vec<SqlValue>,
    ) -> Result<SqlResponse, SqlError> {
        let queued = enqueue(worker, sql, params);
        release_job(parked, release);
        match queued {
            EnqueueResult::Accepted(receiver) => receiver.blocking_recv().expect("reply"),
            EnqueueResult::Full => panic!("full"),
            EnqueueResult::Closed => panic!("closed"),
        }
    }
}
