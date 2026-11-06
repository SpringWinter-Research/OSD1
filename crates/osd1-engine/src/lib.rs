//! One OS thread, one SQLite connection, one bounded FIFO queue.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, RecvError, SyncSender, TrySendError};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Instant;

use osd1_types::{
    EnqueueResult, SqlError, SqlExecutor, SqlRequest, SqlResponse, SqlValue, StatementMeta,
    StatementResult, SERVED_BY_REGION,
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

enum Job {
    Execute {
        request: SqlRequest,
        reply: tokio::sync::oneshot::Sender<Result<SqlResponse, SqlError>>,
    },
    /// Prepare only. The SQL thread replies with whether any statement writes.
    Writes {
        request: SqlRequest,
        reply: mpsc::Sender<Result<bool, SqlError>>,
    },
    /// Run a read without `BEGIN IMMEDIATE`. A write is refused and not stepped.
    Read {
        request: SqlRequest,
        reply: tokio::sync::oneshot::Sender<Result<SqlResponse, SqlError>>,
    },
}

pub struct SqliteWorker {
    tx: Option<SyncSender<Job>>,
    handle: Option<JoinHandle<()>>,
    inflight: Arc<AtomicUsize>,
    capacity: usize,
    /// Read by the authorizer. Default off, so date and random functions stay allowed.
    deny_nondeterministic: Arc<AtomicBool>,
    /// Bumped when the flag changes. Cached statements skip the authorizer.
    auth_generation: Arc<AtomicUsize>,
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
        let inflight = Arc::new(AtomicUsize::new(0));
        let inflight_worker = Arc::clone(&inflight);
        let deny_nondeterministic = Arc::new(AtomicBool::new(false));
        let deny_nondeterministic_worker = Arc::clone(&deny_nondeterministic);
        let auth_generation = Arc::new(AtomicUsize::new(0));
        let auth_generation_worker = Arc::clone(&auth_generation);
        let handle = thread::Builder::new()
            .name("osd1-sql".to_string())
            .spawn(move || {
                worker_main(
                    path,
                    rx,
                    pause,
                    served_by_colo,
                    ready_tx,
                    inflight_worker,
                    deny_nondeterministic_worker,
                    auth_generation_worker,
                )
            })
            .map_err(open_error)?;
        match ready_rx.recv() {
            Ok(Ok(())) => Ok(SqliteWorker {
                tx: Some(tx),
                handle: Some(handle),
                inflight,
                capacity,
                deny_nondeterministic,
                auth_generation,
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

    /// Prepare `request` on the SQL thread and report whether any statement writes.
    /// Does not step and does not start a transaction.
    pub fn writes(&self, request: SqlRequest) -> Result<bool, SqlError> {
        let Some(tx) = &self.tx else {
            return Err(SqlError::ShuttingDown);
        };
        let (reply_tx, reply_rx) = mpsc::channel();
        // Same slot accounting as `enqueue`: the job occupies the bounded queue.
        self.inflight.fetch_add(1, Ordering::Relaxed);
        match tx.try_send(Job::Writes {
            request,
            reply: reply_tx,
        }) {
            Ok(()) => reply_rx.recv().unwrap_or(Err(SqlError::ShuttingDown)),
            Err(TrySendError::Full(_)) => {
                self.inflight.fetch_sub(1, Ordering::Relaxed);
                Err(SqlError::QueueFull)
            }
            Err(TrySendError::Disconnected(_)) => {
                self.inflight.fetch_sub(1, Ordering::Relaxed);
                Err(SqlError::ShuttingDown)
            }
        }
    }

    /// Run a read on the SQL thread without `BEGIN IMMEDIATE`.
    /// A statement that writes is not stepped.
    pub fn enqueue_read(&self, request: SqlRequest) -> EnqueueResult {
        self.enqueue_job(|reply| Job::Read { request, reply })
    }

    /// Wait until the SQL thread can take the job, then wait for the result.
    /// Log apply uses this so a full client queue cannot drop a committed command.
    pub fn execute_blocking(&self, request: SqlRequest) -> Result<SqlResponse, SqlError> {
        let Some(tx) = &self.tx else {
            return Err(SqlError::ShuttingDown);
        };
        let (reply, receiver) = tokio::sync::oneshot::channel();
        self.inflight.fetch_add(1, Ordering::Relaxed);
        if tx.send(Job::Execute { request, reply }).is_err() {
            self.inflight.fetch_sub(1, Ordering::Relaxed);
            return Err(SqlError::ShuttingDown);
        }
        receiver
            .blocking_recv()
            .unwrap_or(Err(SqlError::ShuttingDown))
    }

    fn enqueue_job(
        &self,
        job: impl FnOnce(tokio::sync::oneshot::Sender<Result<SqlResponse, SqlError>>) -> Job,
    ) -> EnqueueResult {
        let Some(tx) = &self.tx else {
            return EnqueueResult::Closed;
        };
        let (reply, receiver) = tokio::sync::oneshot::channel();
        self.inflight.fetch_add(1, Ordering::Relaxed);
        match tx.try_send(job(reply)) {
            Ok(()) => EnqueueResult::Accepted(receiver),
            Err(TrySendError::Full(_)) => {
                self.inflight.fetch_sub(1, Ordering::Relaxed);
                EnqueueResult::Full
            }
            Err(TrySendError::Disconnected(_)) => {
                self.inflight.fetch_sub(1, Ordering::Relaxed);
                EnqueueResult::Closed
            }
        }
    }

    /// Reject nondeterministic SQLite functions when `enabled`.
    /// Off by default. `last_insert_rowid` stays allowed.
    pub fn deny_nondeterministic(&self, enabled: bool) {
        let previous = self.deny_nondeterministic.swap(enabled, Ordering::Release);
        if previous != enabled {
            self.auth_generation.fetch_add(1, Ordering::Release);
        }
    }
}

impl SqlExecutor for SqliteWorker {
    fn enqueue(&self, request: SqlRequest) -> EnqueueResult {
        // Count before send so the worker cannot finish the job first and wrap the counter.
        self.enqueue_job(|reply| Job::Execute { request, reply })
    }

    fn sql_inflight(&self) -> usize {
        self.inflight.load(Ordering::Relaxed)
    }

    fn sql_queue_capacity(&self) -> usize {
        self.capacity
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
    let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    else {
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
    inflight: Arc<AtomicUsize>,
    deny_nondeterministic: Arc<AtomicBool>,
    auth_generation: Arc<AtomicUsize>,
) {
    let conn = match Connection::open(&path) {
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
    let deny_code = Arc::new(AtomicU8::new(DENY_NONE));
    if let Err(error) = install_authorizer(
        &conn,
        Arc::clone(&internal),
        Arc::clone(&schema_changed),
        Arc::clone(&deny_code),
        deny_nondeterministic,
    ) {
        let _ = ready.send(Err(error));
        return;
    }
    // Room for BEGIN, COMMIT, ROLLBACK, and the hot statements.
    conn.set_prepared_statement_cache_capacity(32);
    if ready.send(Ok(())).is_err() {
        return;
    }

    let colo: Arc<str> = Arc::from(served_by_colo);
    let region: Arc<str> = Arc::from(SERVED_BY_REGION);
    let mut size_after = fs::metadata(&path).map(|meta| meta.len()).unwrap_or(0);
    let mut column_cache = ColumnCache::new();
    let mut poisoned = false;
    let mut seen_auth = 0usize;
    loop {
        let job = match rx.recv() {
            Ok(job) => job,
            Err(RecvError) => break,
        };
        let guard = InflightGuard(&inflight);
        if let Some(gate) = &pause {
            if gate.parked.send(()).is_err() || gate.release.recv().is_err() {
                break;
            }
        }
        let auth = auth_generation.load(Ordering::Acquire);
        if auth != seen_auth {
            // prepare_cached does not call the authorizer again.
            conn.flush_prepared_statement_cache();
            seen_auth = auth;
        }
        match job {
            Job::Execute { request, reply } => {
                let result = if poisoned {
                    Err(SqlError::TransactionLeftOpen)
                } else {
                    execute_request(
                        &conn,
                        &path,
                        &colo,
                        &region,
                        &mut size_after,
                        &mut column_cache,
                        &internal,
                        &schema_changed,
                        &deny_code,
                        request,
                    )
                };
                if matches!(result, Err(SqlError::TransactionLeftOpen)) {
                    poisoned = true;
                }
                drop(guard);
                let _ = reply.send(result);
            }
            Job::Writes { request, reply } => {
                let result = if poisoned {
                    Err(SqlError::TransactionLeftOpen)
                } else {
                    request_writes(&conn, &request, &schema_changed, &deny_code)
                };
                drop(guard);
                let _ = reply.send(result);
            }
            Job::Read { request, reply } => {
                let result = if poisoned {
                    Err(SqlError::TransactionLeftOpen)
                } else {
                    read_request(
                        &conn,
                        &request,
                        &colo,
                        &region,
                        &mut column_cache,
                        &size_after,
                        &schema_changed,
                        &deny_code,
                    )
                };
                drop(guard);
                let _ = reply.send(result);
            }
        }
    }
}

struct InflightGuard<'a>(&'a AtomicUsize);

impl Drop for InflightGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
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

const DENY_NONE: u8 = 0;
const DENY_OTHER_DB: u8 = 1;
const DENY_ATTACH: u8 = 2;
const DENY_DETACH: u8 = 3;
const DENY_TRANSACTION: u8 = 4;
const DENY_WRITABLE_SCHEMA: u8 = 5;
const DENY_FUNCTION: u8 = 6;
const DENY_NONDETERMINISTIC: u8 = 7;

fn install_authorizer(
    conn: &Connection,
    internal: Arc<AtomicBool>,
    schema_changed: Arc<AtomicBool>,
    deny_code: Arc<AtomicU8>,
    deny_nondeterministic: Arc<AtomicBool>,
) -> Result<(), rusqlite::Error> {
    // The callback runs on this thread. `internal`, `schema_changed`, and
    // `deny_code` stay on this thread, so Relaxed is enough.
    // `deny_nondeterministic` is stored from other threads.
    conn.authorizer(Some(move |context: AuthContext<'_>| {
        if is_schema_action(&context.action) {
            schema_changed.store(true, Ordering::Relaxed);
        }
        match deny_context(
            &context,
            internal.load(Ordering::Relaxed),
            deny_nondeterministic.load(Ordering::Acquire),
        ) {
            DENY_NONE => Authorization::Allow,
            code => {
                deny_code.store(code, Ordering::Relaxed);
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

fn deny_context(context: &AuthContext<'_>, internal: bool, deny_nondeterministic: bool) -> u8 {
    if let Some(database) = context.database_name.filter(|name| !name.is_empty()) {
        if !database.eq_ignore_ascii_case("main") && !database.eq_ignore_ascii_case("temp") {
            return DENY_OTHER_DB;
        }
    }
    let action = &context.action;
    match action {
        AuthAction::Attach { .. } => DENY_ATTACH,
        AuthAction::Detach { .. } => DENY_DETACH,
        AuthAction::Transaction { .. } | AuthAction::Savepoint { .. } if !internal => {
            DENY_TRANSACTION
        }
        AuthAction::Pragma { pragma_name, .. }
            if pragma_name.eq_ignore_ascii_case("writable_schema") =>
        {
            DENY_WRITABLE_SCHEMA
        }
        AuthAction::Function { function_name } if is_denied_function(function_name) => {
            DENY_FUNCTION
        }
        AuthAction::Function { function_name }
            if deny_nondeterministic && is_nondeterministic_function(function_name) =>
        {
            DENY_NONDETERMINISTIC
        }
        AuthAction::AlterTable { database_name, .. }
            if !database_name.eq_ignore_ascii_case("main")
                && !database_name.eq_ignore_ascii_case("temp") =>
        {
            DENY_OTHER_DB
        }
        _ => DENY_NONE,
    }
}

fn deny_message(code: u8) -> &'static str {
    match code {
        DENY_OTHER_DB => "only the main database is allowed",
        DENY_ATTACH => "ATTACH is not allowed",
        DENY_DETACH => "DETACH is not allowed",
        DENY_TRANSACTION => "client transactions are not allowed",
        DENY_WRITABLE_SCHEMA => "writable_schema is not allowed",
        DENY_FUNCTION => "function is not allowed",
        DENY_NONDETERMINISTIC => "nondeterministic function is not allowed",
        _ => "not authorized",
    }
}

fn is_denied_function(name: &str) -> bool {
    name.eq_ignore_ascii_case("load_extension")
        || name.eq_ignore_ascii_case("readfile")
        || name.eq_ignore_ascii_case("writefile")
}

fn is_nondeterministic_function(name: &str) -> bool {
    const NAMES: &[&str] = &[
        "random",
        "randomblob",
        "datetime",
        "date",
        "time",
        "strftime",
        "julianday",
        "unixepoch",
        "current_time",
        "current_date",
        "current_timestamp",
    ];
    NAMES.iter().any(|denied| name.eq_ignore_ascii_case(denied))
}

fn execute_request(
    conn: &Connection,
    path: &Path,
    colo: &Arc<str>,
    region: &Arc<str>,
    size_after: &mut u64,
    column_cache: &mut ColumnCache,
    internal: &AtomicBool,
    schema_changed: &AtomicBool,
    deny_code: &AtomicU8,
    request: SqlRequest,
) -> Result<SqlResponse, SqlError> {
    deny_code.store(DENY_NONE, Ordering::Relaxed);
    exec_cached(conn, "BEGIN IMMEDIATE", internal, deny_code)?;

    let statements = match run_request(
        conn,
        &request,
        colo,
        region,
        column_cache,
        schema_changed,
        deny_code,
    ) {
        Ok(statements) => statements,
        Err(error) => {
            discard_schema_cache(conn, schema_changed, column_cache);
            rollback(conn, internal, deny_code);
            return Err(error);
        }
    };
    discard_schema_cache(conn, schema_changed, column_cache);

    if let Err(error) = exec_cached(conn, "COMMIT", internal, deny_code) {
        rollback(conn, internal, deny_code);
        return Err(error);
    }
    if !conn.is_autocommit() {
        rollback(conn, internal, deny_code);
        if !conn.is_autocommit() {
            return Err(SqlError::TransactionLeftOpen);
        }
    }

    if statements.iter().any(|statement| statement.meta.changed_db) {
        *size_after = fs::metadata(path)
            .map(|meta| meta.len())
            .unwrap_or(*size_after);
    }
    let mut statements = statements;
    for statement in &mut statements {
        statement.meta.size_after = *size_after;
    }
    Ok(SqlResponse { statements })
}

/// A follower read. Does not call `execute_request` and does not take the write lock.
fn read_request(
    conn: &Connection,
    request: &SqlRequest,
    colo: &Arc<str>,
    region: &Arc<str>,
    column_cache: &mut ColumnCache,
    size_after: &u64,
    schema_changed: &AtomicBool,
    deny_code: &AtomicU8,
) -> Result<SqlResponse, SqlError> {
    if request_writes(conn, request, schema_changed, deny_code)? {
        return Err(SqlError::Sqlite {
            code: 1,
            message: "write must be committed by the leader".to_string(),
        });
    }
    let mut statements = run_request(
        conn,
        request,
        colo,
        region,
        column_cache,
        schema_changed,
        deny_code,
    )?;
    for statement in &mut statements {
        statement.meta.served_by_primary = false;
        statement.meta.size_after = *size_after;
    }
    Ok(SqlResponse { statements })
}

fn run_request(
    conn: &Connection,
    request: &SqlRequest,
    colo: &Arc<str>,
    region: &Arc<str>,
    column_cache: &mut ColumnCache,
    schema_changed: &AtomicBool,
    deny_code: &AtomicU8,
) -> Result<Vec<StatementResult>, SqlError> {
    match sql_shape(&request.sql) {
        SqlShape::Empty => Err(empty_sql()),
        SqlShape::One => run_one(
            conn,
            request,
            colo,
            region,
            column_cache,
            schema_changed,
            deny_code,
        ),
        // A semicolon with more SQL, including a trigger body, stays here so
        // every statement still runs. prepare_cached would keep only the first.
        SqlShape::Script => run_batch(conn, request, colo, region, schema_changed, deny_code),
    }
}

fn empty_sql() -> SqlError {
    SqlError::Sqlite {
        code: 1,
        message: "empty sql".to_string(),
    }
}

/// Prepare each statement and read `readonly`. Nothing is stepped.
/// A script uses `Batch`, same as `run_batch`, so a trigger body stays one statement.
fn request_writes(
    conn: &Connection,
    request: &SqlRequest,
    schema_changed: &AtomicBool,
    deny_code: &AtomicU8,
) -> Result<bool, SqlError> {
    // Preparing CREATE notifies the authorizer. Put the flag back so this
    // check does not look like a schema change to the next request.
    let prior_schema = schema_changed.load(Ordering::Relaxed);
    deny_code.store(DENY_NONE, Ordering::Relaxed);
    let result = match sql_shape(&request.sql) {
        SqlShape::Empty => Err(empty_sql()),
        SqlShape::One => statement_writes(conn, &request.sql, deny_code),
        SqlShape::Script => batch_writes(conn, &request.sql, deny_code),
    };
    schema_changed.store(prior_schema, Ordering::Relaxed);
    if result.is_ok() {
        deny_code.store(DENY_NONE, Ordering::Relaxed);
    }
    result
}

fn statement_writes(conn: &Connection, sql: &str, deny_code: &AtomicU8) -> Result<bool, SqlError> {
    let stmt = conn
        .prepare(sql)
        .map_err(|error| map_sqlite_error(error, deny_code))?;
    let writes = !stmt.readonly();
    drop(stmt);
    Ok(writes)
}

fn batch_writes(conn: &Connection, sql: &str, deny_code: &AtomicU8) -> Result<bool, SqlError> {
    let mut batch = Batch::new(conn, sql);
    let mut saw_statement = false;
    loop {
        match batch.next() {
            Ok(Some(stmt)) => {
                saw_statement = true;
                let writes = !stmt.readonly();
                drop(stmt);
                if writes {
                    return Ok(true);
                }
            }
            Ok(None) => break,
            Err(error) => return Err(map_sqlite_error(error, deny_code)),
        }
    }
    if !saw_statement {
        return Err(empty_sql());
    }
    Ok(false)
}

fn run_one(
    conn: &Connection,
    request: &SqlRequest,
    colo: &Arc<str>,
    region: &Arc<str>,
    column_cache: &mut ColumnCache,
    schema_changed: &AtomicBool,
    deny_code: &AtomicU8,
) -> Result<Vec<StatementResult>, SqlError> {
    // Clear before prepare. CREATE and ALTER notify the authorizer while preparing.
    schema_changed.store(false, Ordering::Relaxed);
    let mut stmt = conn
        .prepare_cached(&request.sql)
        .map_err(|error| map_sqlite_error(error, deny_code))?;
    let columns = cached_columns(&stmt, request.sql.trim(), column_cache);
    let result = run_statement(
        conn,
        &mut stmt,
        &request.params,
        columns,
        colo,
        region,
        schema_changed,
        deny_code,
    )?;
    drop(stmt);
    Ok(vec![result])
}

fn run_batch(
    conn: &Connection,
    request: &SqlRequest,
    colo: &Arc<str>,
    region: &Arc<str>,
    schema_changed: &AtomicBool,
    deny_code: &AtomicU8,
) -> Result<Vec<StatementResult>, SqlError> {
    let mut batch = Batch::new(conn, &request.sql);
    let mut statements = Vec::new();
    let mut any_schema = false;
    loop {
        schema_changed.store(false, Ordering::Relaxed);
        let mut stmt = match batch.next() {
            Ok(Some(stmt)) => stmt,
            Ok(None) => break,
            Err(error) => {
                if any_schema {
                    schema_changed.store(true, Ordering::Relaxed);
                }
                return Err(map_sqlite_error(error, deny_code));
            }
        };
        let columns = read_column_names(&stmt);
        let result = match run_statement(
            conn,
            &mut stmt,
            &request.params,
            columns,
            colo,
            region,
            schema_changed,
            deny_code,
        ) {
            Ok(result) => result,
            Err(error) => {
                if any_schema || schema_changed.load(Ordering::Relaxed) {
                    schema_changed.store(true, Ordering::Relaxed);
                }
                return Err(error);
            }
        };
        if schema_changed.load(Ordering::Relaxed) {
            any_schema = true;
        }
        statements.push(result);
    }
    if any_schema {
        schema_changed.store(true, Ordering::Relaxed);
    }
    if statements.is_empty() {
        return Err(empty_sql());
    }
    Ok(statements)
}

fn run_statement(
    conn: &Connection,
    stmt: &mut rusqlite::Statement<'_>,
    params: &[SqlValue],
    columns: Arc<[String]>,
    colo: &Arc<str>,
    region: &Arc<str>,
    schema_changed: &AtomicBool,
    deny_code: &AtomicU8,
) -> Result<StatementResult, SqlError> {
    let total_before = conn.total_changes();
    let started = Instant::now();
    let rows = read_rows(stmt, params, columns.len(), deny_code)?;
    let rows_read = rows.len() as u64;
    let changes = conn.total_changes().saturating_sub(total_before);
    let duration_ms = started.elapsed().as_secs_f64() * 1000.0;
    Ok(StatementResult {
        columns,
        rows,
        success: true,
        meta: StatementMeta {
            changed_db: changes > 0 || schema_changed.load(Ordering::Relaxed),
            changes,
            duration_ms,
            last_row_id: conn.last_insert_rowid(),
            rows_read,
            rows_written: changes,
            size_after: 0,
            served_by_colo: Arc::clone(colo),
            served_by_region: Arc::clone(region),
            served_by_primary: true,
        },
    })
}

fn read_rows(
    stmt: &mut rusqlite::Statement<'_>,
    params: &[SqlValue],
    column_count: usize,
    deny_code: &AtomicU8,
) -> Result<Vec<Vec<SqlValue>>, SqlError> {
    let bound = bind_params(stmt, params)?;
    let mut rows_iter = stmt
        .query(params_from_iter(bound))
        .map_err(|error| map_sqlite_error(error, deny_code))?;
    let mut rows = Vec::new();
    loop {
        let next = rows_iter
            .next()
            .map_err(|error| map_sqlite_error(error, deny_code))?;
        let Some(row) = next else { break };
        let mut values = Vec::with_capacity(column_count);
        for index in 0..column_count {
            let value = row
                .get_ref(index)
                .map_err(|error| map_sqlite_error(error, deny_code))?;
            values.push(value_from_ref(value)?);
        }
        rows.push(values);
    }
    Ok(rows)
}

fn exec_cached(
    conn: &Connection,
    sql: &'static str,
    internal: &AtomicBool,
    deny_code: &AtomicU8,
) -> Result<(), SqlError> {
    allow_internal(internal, || {
        let mut stmt = conn
            .prepare_cached(sql)
            .map_err(|error| map_sqlite_error(error, deny_code))?;
        stmt.execute(params_from_iter(std::iter::empty::<SqliteValue>()))
            .map(|_| ())
            .map_err(|error| map_sqlite_error(error, deny_code))
    })
}

fn discard_schema_cache(
    conn: &Connection,
    schema_changed: &AtomicBool,
    column_cache: &mut ColumnCache,
) {
    if schema_changed.swap(false, Ordering::Relaxed) {
        column_cache.clear();
        conn.flush_prepared_statement_cache();
    }
}

const COLUMN_CACHE_CAP: usize = 16;

struct ColumnCache {
    entries: Vec<(Arc<str>, Arc<[String]>)>,
}

impl ColumnCache {
    fn new() -> Self {
        Self {
            entries: Vec::with_capacity(COLUMN_CACHE_CAP),
        }
    }

    fn clear(&mut self) {
        self.entries.clear();
    }

    fn get(&mut self, sql: &str) -> Option<Arc<[String]>> {
        let pos = self
            .entries
            .iter()
            .position(|(key, _)| key.as_ref() == sql)?;
        let hit = Arc::clone(&self.entries[pos].1);
        if pos + 1 != self.entries.len() {
            let entry = self.entries.remove(pos);
            self.entries.push(entry);
        }
        Some(hit)
    }

    fn insert(&mut self, sql: &str, columns: Arc<[String]>) {
        if self.entries.len() == COLUMN_CACHE_CAP {
            self.entries.remove(0);
        }
        self.entries.push((Arc::from(sql), columns));
    }
}

fn cached_columns(
    stmt: &rusqlite::Statement<'_>,
    sql: &str,
    cache: &mut ColumnCache,
) -> Arc<[String]> {
    if let Some(hit) = cache.get(sql) {
        return hit;
    }
    let names = read_column_names(stmt);
    cache.insert(sql, Arc::clone(&names));
    names
}

fn read_column_names(stmt: &rusqlite::Statement<'_>) -> Arc<[String]> {
    let names: Vec<String> = (0..stmt.column_count())
        .map(|index| stmt.column_name(index).unwrap_or("").to_string())
        .collect();
    Arc::from(names)
}

enum SqlShape {
    Empty,
    One,
    Script,
}

fn sql_shape(sql: &str) -> SqlShape {
    let bytes = sql.as_bytes();
    let mut i = 0;
    let mut saw_sql = false;
    let mut depth = 0;
    while i < bytes.len() {
        if depth > 0 {
            if bytes[i] == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
                depth += 1;
                i += 2;
                continue;
            }
            if bytes[i] == b'*' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
                depth -= 1;
                i += 2;
                continue;
            }
            i += 1;
            continue;
        }
        match bytes[i] {
            b' ' | b'\t' | b'\n' | b'\r' | 0x0c => i += 1,
            b'-' if i + 1 < bytes.len() && bytes[i + 1] == b'-' => {
                i += 2;
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'*' => {
                depth = 1;
                i += 2;
            }
            b'\'' | b'"' | b'`' => {
                saw_sql = true;
                i = skip_quote(bytes, i, bytes[i]);
            }
            b'[' => {
                saw_sql = true;
                i += 1;
                while i < bytes.len() && bytes[i] != b']' {
                    i += 1;
                }
                if i < bytes.len() {
                    i += 1;
                }
            }
            b';' => {
                return if rest_has_sql(&bytes[i + 1..]) {
                    SqlShape::Script
                } else if saw_sql {
                    SqlShape::One
                } else {
                    SqlShape::Empty
                };
            }
            _ => {
                saw_sql = true;
                i += 1;
            }
        }
    }
    if saw_sql {
        SqlShape::One
    } else {
        SqlShape::Empty
    }
}

fn rest_has_sql(bytes: &[u8]) -> bool {
    let mut i = 0;
    let mut depth = 0;
    while i < bytes.len() {
        if depth > 0 {
            if bytes[i] == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
                depth += 1;
                i += 2;
                continue;
            }
            if bytes[i] == b'*' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
                depth -= 1;
                i += 2;
                continue;
            }
            i += 1;
            continue;
        }
        match bytes[i] {
            b' ' | b'\t' | b'\n' | b'\r' | 0x0c | b';' => i += 1,
            b'-' if i + 1 < bytes.len() && bytes[i + 1] == b'-' => {
                i += 2;
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'*' => {
                depth = 1;
                i += 2;
            }
            _ => return true,
        }
    }
    false
}

fn skip_quote(bytes: &[u8], mut i: usize, quote: u8) -> usize {
    i += 1;
    while i < bytes.len() {
        if bytes[i] == quote {
            if i + 1 < bytes.len() && bytes[i + 1] == quote {
                i += 2;
                continue;
            }
            return i + 1;
        }
        i += 1;
    }
    bytes.len()
}

fn bind_params(
    stmt: &rusqlite::Statement<'_>,
    params: &[SqlValue],
) -> Result<Vec<SqliteValue>, SqlError> {
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

fn rollback(conn: &Connection, internal: &AtomicBool, deny_code: &AtomicU8) {
    let _ = exec_cached(conn, "ROLLBACK", internal, deny_code);
}

fn allow_internal<T>(internal: &AtomicBool, f: impl FnOnce() -> T) -> T {
    internal.store(true, Ordering::Relaxed);
    struct Clear<'a>(&'a AtomicBool);
    impl Drop for Clear<'_> {
        fn drop(&mut self) {
            self.0.store(false, Ordering::Relaxed);
        }
    }
    let _clear = Clear(internal);
    f()
}

fn map_sqlite_error(error: rusqlite::Error, deny_code: &AtomicU8) -> SqlError {
    let (code, message) = match &error {
        rusqlite::Error::SqliteFailure(ffi, detail) => (
            ffi.extended_code,
            detail.clone().unwrap_or_else(|| error.to_string()),
        ),
        other => (1, other.to_string()),
    };
    if code == 23 || code & 0xff == 23 {
        let reason = deny_code.swap(DENY_NONE, Ordering::Relaxed);
        return SqlError::AuthorizerDenied {
            message: deny_message(reason).to_string(),
        };
    }
    // A function deny is SQLITE_AUTH inside the authorizer, then SQLite's
    // name resolver rewrites it to SQLITE_ERROR ("not authorized to use function").
    if deny_code.load(Ordering::Relaxed) == DENY_NONDETERMINISTIC {
        deny_code.store(DENY_NONE, Ordering::Relaxed);
        return SqlError::AuthorizerDenied {
            message: deny_message(DENY_NONDETERMINISTIC).to_string(),
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
        std::env::temp_dir().join(format!("osd1-{}-{}-{name}.db", std::process::id(), n))
    }

    fn run(
        worker: &SqliteWorker,
        sql: &str,
        params: Vec<SqlValue>,
    ) -> Result<SqlResponse, SqlError> {
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
        assert_eq!(
            response.statements[0].columns.as_ref(),
            ["n".to_string()].as_slice()
        );
        assert_eq!(
            response.statements[0].rows,
            vec![vec![SqlValue::Integer(1)]]
        );
        assert_eq!(response.statements[0].meta.rows_read, 1);
        assert!(response.statements[0].meta.served_by_primary);
        assert_eq!(
            response.statements[0].meta.served_by_region.as_ref(),
            "WNAM"
        );
    }

    #[test]
    fn follower_read_sets_primary_false_and_does_not_run_a_write() {
        let path = temp_db("follower-read");
        let worker = SqliteWorker::open(&path, 4, "LOC").unwrap();
        run(&worker, "CREATE TABLE t(n INTEGER)", vec![]).unwrap();
        run(&worker, "INSERT INTO t(n) VALUES (1)", vec![]).unwrap();
        let response = read(&worker, "SELECT n FROM t", vec![]).unwrap();
        assert_eq!(
            response.statements[0].rows,
            vec![vec![SqlValue::Integer(1)]]
        );
        assert!(!response.statements[0].meta.served_by_primary);
        assert_eq!(
            response.statements[0].meta.served_by_region.as_ref(),
            "WNAM"
        );
        let refused = read(&worker, "INSERT INTO t(n) VALUES (2)", vec![]);
        match refused {
            Err(SqlError::Sqlite { message, .. }) => {
                assert!(
                    message.contains("write must be committed by the leader"),
                    "{message}"
                );
            }
            other => panic!("expected a refused write, got {other:?}"),
        }
        let after = run(&worker, "SELECT n FROM t", vec![]).unwrap();
        assert_eq!(after.statements[0].rows, vec![vec![SqlValue::Integer(1)]]);
        assert!(after.statements[0].meta.served_by_primary);
    }

    fn read(
        worker: &SqliteWorker,
        sql: &str,
        params: Vec<SqlValue>,
    ) -> Result<SqlResponse, SqlError> {
        match worker.enqueue_read(SqlRequest {
            sql: sql.to_string(),
            params,
            shape: ResultShape::Objects,
        }) {
            EnqueueResult::Accepted(receiver) => receiver.blocking_recv().expect("worker reply"),
            EnqueueResult::Full => panic!("queue full"),
            EnqueueResult::Closed => panic!("worker closed"),
        }
    }

    #[test]
    fn a_cached_statement_binds_each_request() {
        let path = temp_db("rebind");
        let worker = SqliteWorker::open(&path, 4, "LOC").unwrap();
        let first = run(&worker, "SELECT ? AS v", vec![SqlValue::Integer(1)]).unwrap();
        let second = run(&worker, "SELECT ? AS v", vec![SqlValue::Integer(2)]).unwrap();
        assert_eq!(first.statements[0].rows, vec![vec![SqlValue::Integer(1)]]);
        assert_eq!(second.statements[0].rows, vec![vec![SqlValue::Integer(2)]]);
        assert_eq!(
            first.statements[0].meta.size_after,
            second.statements[0].meta.size_after
        );
    }

    #[test]
    fn a_script_runs_every_statement_and_a_trailing_semicolon_does_not() {
        let path = temp_db("script");
        let worker = SqliteWorker::open(&path, 4, "LOC").unwrap();
        let one = run(&worker, "SELECT 1;", vec![]).unwrap();
        assert_eq!(one.statements.len(), 1);
        assert_eq!(one.statements[0].rows, vec![vec![SqlValue::Integer(1)]]);
        let quoted = run(&worker, "SELECT ';' AS s", vec![]).unwrap();
        assert_eq!(quoted.statements.len(), 1);
        assert_eq!(
            quoted.statements[0].rows,
            vec![vec![SqlValue::Text(";".into())]]
        );
        let both = run(&worker, "SELECT 1 AS a; SELECT 2 AS b", vec![]).unwrap();
        assert_eq!(both.statements.len(), 2);
        assert_eq!(both.statements[0].rows, vec![vec![SqlValue::Integer(1)]]);
        assert_eq!(both.statements[1].rows, vec![vec![SqlValue::Integer(2)]]);
    }

    #[test]
    fn a_trigger_body_stays_one_statement() {
        let path = temp_db("trigger");
        let worker = SqliteWorker::open(&path, 4, "LOC").unwrap();
        run(&worker, "CREATE TABLE a(n INTEGER)", vec![]).unwrap();
        run(&worker, "CREATE TABLE b(n INTEGER)", vec![]).unwrap();
        run(
            &worker,
            "CREATE TRIGGER t AFTER INSERT ON a BEGIN INSERT INTO b VALUES (new.n); END",
            vec![],
        )
        .unwrap();
        run(&worker, "INSERT INTO a VALUES (7)", vec![]).unwrap();
        let response = run(&worker, "SELECT n FROM b", vec![]).unwrap();
        assert_eq!(
            response.statements[0].rows,
            vec![vec![SqlValue::Integer(7)]]
        );
    }

    #[test]
    fn a_cached_select_sees_a_new_column() {
        let path = temp_db("alter-cache");
        let worker = SqliteWorker::open(&path, 4, "LOC").unwrap();
        run(&worker, "CREATE TABLE t(n INTEGER)", vec![]).unwrap();
        run(&worker, "INSERT INTO t VALUES (1)", vec![]).unwrap();
        let before = run(&worker, "SELECT * FROM t", vec![]).unwrap();
        assert_eq!(
            before.statements[0].columns.as_ref(),
            ["n".to_string()].as_slice()
        );
        run(&worker, "ALTER TABLE t ADD COLUMN m INTEGER", vec![]).unwrap();
        run(&worker, "UPDATE t SET m = 2", vec![]).unwrap();
        let after = run(&worker, "SELECT * FROM t", vec![]).unwrap();
        assert_eq!(
            after.statements[0].columns.as_ref(),
            ["n".to_string(), "m".to_string()].as_slice()
        );
        assert_eq!(
            after.statements[0].rows,
            vec![vec![SqlValue::Integer(1), SqlValue::Integer(2)]]
        );
    }

    #[test]
    fn sql_shape_keeps_scripts_and_trailing_semicolons_apart() {
        assert!(matches!(sql_shape("SELECT 1"), SqlShape::One));
        assert!(matches!(sql_shape("  SELECT 1;  "), SqlShape::One));
        assert!(matches!(sql_shape("SELECT 1; -- tail"), SqlShape::One));
        assert!(matches!(sql_shape("SELECT ';';"), SqlShape::One));
        assert!(matches!(sql_shape("/* only */"), SqlShape::Empty));
        assert!(matches!(sql_shape(";;;"), SqlShape::Empty));
        assert!(matches!(sql_shape("SELECT 1; SELECT 2"), SqlShape::Script));
        assert!(matches!(
            sql_shape("CREATE TRIGGER t AFTER INSERT ON a BEGIN INSERT INTO b VALUES (new.n); END"),
            SqlShape::Script
        ));
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
        let response = run_paused(
            &worker,
            &parked_rx,
            &release_tx,
            "SELECT n FROM t ORDER BY rowid",
            vec![],
        )
        .unwrap();
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
    fn inflight_includes_the_running_request_only() {
        let path = temp_db("inflight");
        let (parked_tx, parked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let worker = SqliteWorker::open_with_pause(
            &path,
            2,
            "LOC",
            PauseGate {
                parked: parked_tx,
                release: release_rx,
            },
        )
        .unwrap();
        assert_eq!(worker.sql_inflight(), 0);
        assert_eq!(worker.sql_queue_capacity(), 2);
        let queued = enqueue(&worker, "SELECT 1", vec![]);
        parked_rx.recv().unwrap();
        assert_eq!(worker.sql_inflight(), 1);
        release_tx.send(()).unwrap();
        accept(queued).blocking_recv().unwrap().unwrap();
        assert_eq!(worker.sql_inflight(), 0);
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
        assert!(
            matches!(error, SqlError::AuthorizerDenied { .. }),
            "{error:?}"
        );
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
        assert_eq!(
            response.statements[0].rows,
            vec![vec![SqlValue::Integer(1)]]
        );
    }

    #[test]
    fn writable_schema_is_denied() {
        let path = temp_db("pragma");
        let worker = SqliteWorker::open(&path, 4, "LOC").unwrap();
        let error = run(&worker, "PRAGMA writable_schema=ON", vec![]).unwrap_err();
        assert!(
            matches!(error, SqlError::AuthorizerDenied { .. }),
            "{error:?}"
        );
    }

    #[test]
    fn schema_catalog_and_table_info_are_readable() {
        let path = temp_db("catalog");
        let worker = SqliteWorker::open(&path, 4, "LOC").unwrap();
        run(
            &worker,
            "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)",
            vec![],
        )
        .unwrap();
        let master = run(
            &worker,
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'items'",
            vec![],
        )
        .unwrap();
        assert_eq!(
            master.statements[0].rows,
            vec![vec![SqlValue::Text("items".to_string())]]
        );
        let info = run(&worker, r#"PRAGMA table_info("items")"#, vec![]).unwrap();
        assert_eq!(
            info.statements[0].columns.as_ref(),
            [
                "cid".to_string(),
                "name".to_string(),
                "type".to_string(),
                "notnull".to_string(),
                "dflt_value".to_string(),
                "pk".to_string(),
            ]
            .as_slice()
        );
        let names: Vec<_> = info.statements[0]
            .rows
            .iter()
            .map(|row| row[1].clone())
            .collect();
        assert_eq!(
            names,
            vec![
                SqlValue::Text("id".to_string()),
                SqlValue::Text("name".to_string()),
            ]
        );
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

    #[test]
    fn select_does_not_write() {
        let path = temp_db("select-writes");
        let worker = SqliteWorker::open(&path, 4, "LOC").unwrap();
        assert!(!worker.writes(sql_request("SELECT 1")).unwrap());
        assert!(!worker.writes(sql_request("SELECT 1; SELECT 2")).unwrap());
        let from_execute = run(&worker, "", vec![]).unwrap_err();
        let from_writes = worker.writes(sql_request("")).unwrap_err();
        assert_eq!(from_execute, from_writes);
    }

    #[test]
    fn insert_writes() {
        let path = temp_db("insert-writes");
        let worker = SqliteWorker::open(&path, 4, "LOC").unwrap();
        run(&worker, "CREATE TABLE t(n INTEGER)", vec![]).unwrap();
        assert!(worker
            .writes(sql_request("INSERT INTO t VALUES (1)"))
            .unwrap());
        assert!(worker.writes(sql_request("UPDATE t SET n = 2")).unwrap());
        assert!(worker.writes(sql_request("DELETE FROM t")).unwrap());
        assert!(worker
            .writes(sql_request("CREATE TABLE u(n INTEGER)"))
            .unwrap());
        let response = run(&worker, "SELECT COUNT(*) FROM t", vec![]).unwrap();
        assert_eq!(
            response.statements[0].rows,
            vec![vec![SqlValue::Integer(0)]]
        );
        let missing = run(&worker, "SELECT * FROM u", vec![]).unwrap_err();
        assert!(matches!(missing, SqlError::Sqlite { .. }), "{missing:?}");
    }

    #[test]
    fn a_script_with_a_write_writes() {
        let path = temp_db("script-writes");
        let worker = SqliteWorker::open(&path, 4, "LOC").unwrap();
        run(&worker, "CREATE TABLE t(n INTEGER)", vec![]).unwrap();
        run(&worker, "CREATE TABLE a(n INTEGER)", vec![]).unwrap();
        run(&worker, "CREATE TABLE b(n INTEGER)", vec![]).unwrap();
        assert!(worker
            .writes(sql_request("SELECT 1; INSERT INTO t VALUES (1)"))
            .unwrap());
        let response = run(&worker, "SELECT COUNT(*) FROM t", vec![]).unwrap();
        assert_eq!(
            response.statements[0].rows,
            vec![vec![SqlValue::Integer(0)]]
        );
        assert!(worker
            .writes(sql_request(
                "CREATE TRIGGER tr AFTER INSERT ON a BEGIN INSERT INTO b VALUES (new.n); END",
            ))
            .unwrap());
        let trigger = run(
            &worker,
            "SELECT COUNT(*) FROM sqlite_schema WHERE name = 'tr'",
            vec![],
        )
        .unwrap();
        assert_eq!(trigger.statements[0].rows, vec![vec![SqlValue::Integer(0)]]);
    }

    #[test]
    fn datetime_now_is_allowed_in_simple_mode() {
        let path = temp_db("datetime-simple");
        let worker = SqliteWorker::open(&path, 4, "LOC").unwrap();
        let response = run(&worker, "SELECT datetime('now')", vec![]).unwrap();
        assert!(matches!(
            response.statements[0].rows[0][0],
            SqlValue::Text(_)
        ));
        let random = run(&worker, "SELECT random()", vec![]).unwrap();
        assert!(matches!(
            random.statements[0].rows[0][0],
            SqlValue::Integer(_)
        ));
        run(&worker, "CREATE TABLE t(n INTEGER)", vec![]).unwrap();
        run(&worker, "INSERT INTO t VALUES (1)", vec![]).unwrap();
        let rows = run(&worker, "SELECT n FROM t", vec![]).unwrap();
        assert_eq!(rows.statements[0].rows, vec![vec![SqlValue::Integer(1)]]);
    }

    #[test]
    fn nondeterministic_functions_are_denied_only_when_enabled() {
        let path = temp_db("nondet");
        let worker = SqliteWorker::open(&path, 4, "LOC").unwrap();
        run(&worker, "SELECT datetime('now')", vec![]).unwrap();
        worker.deny_nondeterministic(true);
        let error = run(&worker, "SELECT datetime('now')", vec![]).unwrap_err();
        assert_eq!(
            error,
            SqlError::AuthorizerDenied {
                message: "nondeterministic function is not allowed".to_string(),
            }
        );
        run(&worker, "SELECT last_insert_rowid()", vec![]).unwrap();
        worker.deny_nondeterministic(false);
        run(&worker, "SELECT datetime('now')", vec![]).unwrap();
    }

    fn sql_request(sql: &str) -> SqlRequest {
        SqlRequest {
            sql: sql.to_string(),
            params: Vec::new(),
            shape: ResultShape::Objects,
        }
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
