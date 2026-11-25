//! One OS thread, one SQLite connection, one bounded FIFO queue.

use std::cell::Cell;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, RecvError, SyncSender, TrySendError};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// A follower retries one command id for about 3 seconds. Keep that stored
/// body until a later purge, so the retry does not come back empty.
const APPLIED_BODY_HOLD: Duration = Duration::from_secs(3);

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
    /// Apply one committed command. A repeated `(origin, id)` returns the stored result.
    ExecuteId {
        origin: u64,
        id: u64,
        /// Raft log index. Zero stores `idx` as NULL and is not cleared by a purge.
        log_index: u64,
        request: SqlRequest,
        reply: tokio::sync::oneshot::Sender<Result<SqlResponse, SqlError>>,
    },
    /// Set `body` to NULL for rows whose log index is at or below `index`.
    /// `now` is the caller's clock, so a test can move past [`APPLIED_BODY_HOLD`].
    ForgetBodies {
        index: u64,
        now: Instant,
        reply: tokio::sync::oneshot::Sender<Result<(), SqlError>>,
    },
    /// Copy the main database to `dest` with the SQLite backup API.
    Backup {
        dest: PathBuf,
        reply: mpsc::Sender<Result<(), SqlError>>,
    },
    /// Backup to a sibling file and return those bytes.
    Snapshot {
        reply: mpsc::Sender<Result<Vec<u8>, SqlError>>,
    },
    /// Replace the database file with `bytes` and reopen the one connection.
    Install {
        bytes: Vec<u8>,
        reply: mpsc::Sender<Result<(), SqlError>>,
    },
    /// `MAX(id)` from `osd1_applied` for one origin. Missing table is 0.
    MaxApplied {
        origin: u64,
        reply: mpsc::Sender<Result<u64, SqlError>>,
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

    /// Apply a raft command. The `osd1_applied` row and the user statements commit together.
    /// A second call with the same pair returns the stored result and does not run the SQL.
    pub fn execute_idempotent(
        &self,
        origin: u64,
        id: u64,
        log_index: u64,
        request: SqlRequest,
    ) -> Result<SqlResponse, SqlError> {
        let Some(tx) = &self.tx else {
            return Err(SqlError::ShuttingDown);
        };
        let (reply, receiver) = tokio::sync::oneshot::channel();
        self.inflight.fetch_add(1, Ordering::Relaxed);
        if tx
            .send(Job::ExecuteId {
                origin,
                id,
                log_index,
                request,
                reply,
            })
            .is_err()
        {
            self.inflight.fetch_sub(1, Ordering::Relaxed);
            return Err(SqlError::ShuttingDown);
        }
        receiver
            .blocking_recv()
            .unwrap_or(Err(SqlError::ShuttingDown))
    }

    /// Drop stored response bodies for log indexes at or below `index`.
    /// A body applied within the last 3 seconds stays until a later purge.
    /// The `(origin, id)` row stays, so a later apply returns an empty result.
    /// A busy transaction is left for the next purge.
    pub fn forget_applied_bodies(&self, index: u64) -> Result<(), SqlError> {
        self.forget_applied_bodies_at(index, Instant::now())
    }

    fn forget_applied_bodies_at(&self, index: u64, now: Instant) -> Result<(), SqlError> {
        let Some(tx) = &self.tx else {
            return Err(SqlError::ShuttingDown);
        };
        let (reply, receiver) = tokio::sync::oneshot::channel();
        self.inflight.fetch_add(1, Ordering::Relaxed);
        if tx.send(Job::ForgetBodies { index, now, reply }).is_err() {
            self.inflight.fetch_sub(1, Ordering::Relaxed);
            return Err(SqlError::ShuttingDown);
        }
        receiver
            .blocking_recv()
            .unwrap_or(Err(SqlError::ShuttingDown))
    }

    /// Copy the main database to `dest` with SQLite's online backup API.
    /// The destination is a separate file. The live database keeps its one connection.
    pub fn backup_to(&self, dest: &Path) -> Result<(), SqlError> {
        let Some(tx) = &self.tx else {
            return Err(SqlError::ShuttingDown);
        };
        let (reply_tx, reply_rx) = mpsc::channel();
        self.inflight.fetch_add(1, Ordering::Relaxed);
        if tx
            .send(Job::Backup {
                dest: dest.to_path_buf(),
                reply: reply_tx,
            })
            .is_err()
        {
            self.inflight.fetch_sub(1, Ordering::Relaxed);
            return Err(SqlError::ShuttingDown);
        }
        reply_rx.recv().unwrap_or(Err(SqlError::ShuttingDown))
    }

    /// Consistent copy of the main database file.
    pub fn snapshot_main(&self) -> Result<Vec<u8>, SqlError> {
        let Some(tx) = &self.tx else {
            return Err(SqlError::ShuttingDown);
        };
        let (reply_tx, reply_rx) = mpsc::channel();
        self.inflight.fetch_add(1, Ordering::Relaxed);
        if tx.send(Job::Snapshot { reply: reply_tx }).is_err() {
            self.inflight.fetch_sub(1, Ordering::Relaxed);
            return Err(SqlError::ShuttingDown);
        }
        reply_rx.recv().unwrap_or(Err(SqlError::ShuttingDown))
    }

    /// Highest command id in `osd1_applied` for `origin`, or 0 when that table is absent.
    ///
    /// A restored database keeps this table. A new raft directory does not, so the next
    /// command id has to start after these rows or the first write is skipped.
    pub fn max_applied_id(&self, origin: u64) -> Result<u64, SqlError> {
        let Some(tx) = &self.tx else {
            return Err(SqlError::ShuttingDown);
        };
        let (reply_tx, reply_rx) = mpsc::channel();
        self.inflight.fetch_add(1, Ordering::Relaxed);
        if tx
            .send(Job::MaxApplied {
                origin,
                reply: reply_tx,
            })
            .is_err()
        {
            self.inflight.fetch_sub(1, Ordering::Relaxed);
            return Err(SqlError::ShuttingDown);
        }
        reply_rx.recv().unwrap_or(Err(SqlError::ShuttingDown))
    }

    /// Replace the open database with `bytes`. The SQL thread closes and reopens its connection.
    pub fn install_main(&self, bytes: Vec<u8>) -> Result<(), SqlError> {
        let Some(tx) = &self.tx else {
            return Err(SqlError::ShuttingDown);
        };
        let (reply_tx, reply_rx) = mpsc::channel();
        self.inflight.fetch_add(1, Ordering::Relaxed);
        if tx
            .send(Job::Install {
                bytes,
                reply: reply_tx,
            })
            .is_err()
        {
            self.inflight.fetch_sub(1, Ordering::Relaxed);
            return Err(SqlError::ShuttingDown);
        }
        reply_rx.recv().unwrap_or(Err(SqlError::ShuttingDown))
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
    let deny_code = Arc::new(AtomicU8::new(DENY_NONE));
    if let Err(error) = install_authorizer(
        &conn,
        Arc::clone(&internal),
        Arc::clone(&schema_changed),
        Arc::clone(&deny_code),
        Arc::clone(&deny_nondeterministic),
    ) {
        let _ = ready.send(Err(error));
        return;
    }
    install_progress(&conn);
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
    let mut applied_at: HashMap<u64, Instant> = HashMap::new();
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
                let deadline = request.deadline;
                let result = if poisoned {
                    Err(SqlError::TransactionLeftOpen)
                } else {
                    run_with_deadline(deadline, || {
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
                    })
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
                let deadline = request.deadline;
                let result = if poisoned {
                    Err(SqlError::TransactionLeftOpen)
                } else {
                    run_with_deadline(deadline, || {
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
                    })
                };
                drop(guard);
                let _ = reply.send(result);
            }
            Job::ExecuteId {
                origin,
                id,
                log_index,
                request,
                reply,
            } => {
                let deadline = request.deadline;
                let result = if poisoned {
                    Err(SqlError::TransactionLeftOpen)
                } else {
                    run_with_deadline(deadline, || {
                        execute_idempotent(
                            &conn,
                            &path,
                            &colo,
                            &region,
                            &mut size_after,
                            &mut column_cache,
                            &internal,
                            &schema_changed,
                            &deny_code,
                            origin,
                            id,
                            log_index,
                            request,
                            &mut applied_at,
                        )
                    })
                };
                if matches!(result, Err(SqlError::TransactionLeftOpen)) {
                    poisoned = true;
                }
                drop(guard);
                let _ = reply.send(result);
            }
            Job::Backup { dest, reply } => {
                let result = if poisoned || !conn.is_autocommit() {
                    Err(SqlError::TransactionLeftOpen)
                } else {
                    backup_database(&conn, &dest)
                };
                drop(guard);
                let _ = reply.send(result);
            }
            Job::Snapshot { reply } => {
                let result = if poisoned || !conn.is_autocommit() {
                    Err(SqlError::TransactionLeftOpen)
                } else {
                    snapshot_bytes(&conn, &path)
                };
                drop(guard);
                let _ = reply.send(result);
            }
            Job::Install { bytes, reply } => {
                let result = if poisoned || !conn.is_autocommit() {
                    Err(SqlError::TransactionLeftOpen)
                } else {
                    // The file cannot be replaced while this connection holds it.
                    let closed = std::mem::replace(
                        &mut conn,
                        Connection::open_in_memory().expect("memory sqlite"),
                    );
                    drop(closed);
                    match replace_database_file(&path, &bytes) {
                        Ok(()) => match reopen_connection(
                            &path,
                            &internal,
                            &schema_changed,
                            &deny_code,
                            &deny_nondeterministic,
                        ) {
                            Ok(opened) => {
                                conn = opened;
                                applied_at.clear();
                                column_cache = ColumnCache::new();
                                size_after =
                                    fs::metadata(&path).map(|meta| meta.len()).unwrap_or(0);
                                poisoned = false;
                                Ok(())
                            }
                            Err(error) => {
                                poisoned = true;
                                Err(error)
                            }
                        },
                        Err(error) => {
                            if let Ok(opened) = reopen_connection(
                                &path,
                                &internal,
                                &schema_changed,
                                &deny_code,
                                &deny_nondeterministic,
                            ) {
                                conn = opened;
                            } else {
                                poisoned = true;
                            }
                            Err(error)
                        }
                    }
                };
                drop(guard);
                let _ = reply.send(result);
            }
            Job::MaxApplied { origin, reply } => {
                let result = if poisoned {
                    Err(SqlError::TransactionLeftOpen)
                } else {
                    max_applied_origin(&conn, &internal, origin)
                };
                drop(guard);
                let _ = reply.send(result);
            }
            Job::ForgetBodies { index, now, reply } => {
                let result = if poisoned {
                    Err(SqlError::TransactionLeftOpen)
                } else {
                    clear_applied_bodies(&conn, &internal, index, now, &mut applied_at)
                };
                drop(guard);
                let _ = reply.send(result);
            }
        }
        if !poisoned {
            checkpoint_if_wal_is_large(&conn, &path, &internal);
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
    register_vector_module(conn)?;
    conn.execute_batch(
        "
        PRAGMA page_size=4096;
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=FULL;
        PRAGMA foreign_keys=ON;
        PRAGMA busy_timeout=0;
        ",
    )?;
    let status: String = conn.query_row("PRAGMA quick_check", [], |row| row.get(0))?;
    if status != "ok" {
        return Err(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CORRUPT),
            Some(format!("quick_check: {status}")),
        ));
    }
    Ok(())
}

/// `sqlite-vec` 0.1.9 publishes `sqlite3_vec_init` with an empty Rust signature.
/// `sqlite-vec.h` defines the SQLite extension entry point. The crate builds
/// that file with `SQLITE_CORE`, so a null API pointer uses this process's SQLite.
fn register_vector_module(conn: &Connection) -> Result<(), rusqlite::Error> {
    use rusqlite::auto_extension::RawAutoExtension;

    let rc = unsafe {
        let init: RawAutoExtension = std::mem::transmute(sqlite_vec::sqlite3_vec_init as *const ());
        init(conn.handle(), std::ptr::null_mut(), std::ptr::null())
    };
    if rc != rusqlite::ffi::SQLITE_OK {
        return Err(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rc),
            Some("sqlite-vec init failed".to_string()),
        ));
    }
    Ok(())
}

thread_local! {
    static SQL_DEADLINE: Cell<Option<Instant>> = const { Cell::new(None) };
}

fn run_with_deadline<T>(
    deadline: Option<Instant>,
    body: impl FnOnce() -> Result<T, SqlError>,
) -> Result<T, SqlError> {
    if deadline.is_some_and(|at| Instant::now() >= at) {
        return Err(SqlError::TimedOut);
    }
    SQL_DEADLINE.with(|cell| cell.set(deadline));
    struct Clear;
    impl Drop for Clear {
        fn drop(&mut self) {
            SQL_DEADLINE.with(|cell| cell.set(None));
        }
    }
    let _clear = Clear;
    body()
}

/// `sqlite3_progress_handler`. Returning true interrupts the statement.
fn install_progress(conn: &Connection) {
    conn.progress_handler(
        1_000,
        Some(|| SQL_DEADLINE.with(|cell| cell.get().is_some_and(|at| Instant::now() >= at))),
    );
}

const WAL_CHECKPOINT_BYTES: u64 = 4 * 1024 * 1024;

fn checkpoint_if_wal_is_large(conn: &Connection, path: &Path, internal: &AtomicBool) {
    if !conn.is_autocommit() {
        return;
    }
    let wal = PathBuf::from(format!("{}-wal", path.display()));
    let len = fs::metadata(&wal).map(|meta| meta.len()).unwrap_or(0);
    if len <= WAL_CHECKPOINT_BYTES {
        return;
    }
    let _ = allow_internal(internal, || {
        conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE)")
    });
}

const DENY_NONE: u8 = 0;
const DENY_OTHER_DB: u8 = 1;
const DENY_ATTACH: u8 = 2;
const DENY_DETACH: u8 = 3;
const DENY_TRANSACTION: u8 = 4;
const DENY_WRITABLE_SCHEMA: u8 = 5;
const DENY_FUNCTION: u8 = 6;
const DENY_NONDETERMINISTIC: u8 = 7;
const DENY_APPLIED: u8 = 8;
const DENY_DURABILITY: u8 = 9;
const APPLIED_TABLE: &str = "osd1_applied";

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
    if !internal && touches_applied_table(&context.action) {
        return DENY_APPLIED;
    }
    // DROP COLUMN stores the column name in the database-name slot.
    // The database name for ALTER TABLE is on the action.
    let database = match &context.action {
        AuthAction::AlterTable { .. } => None,
        _ => context.database_name,
    };
    if let Some(database) = database.filter(|name| !name.is_empty()) {
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
        AuthAction::Pragma { pragma_name, .. } if is_durability_pragma(pragma_name) => {
            DENY_DURABILITY
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
        DENY_APPLIED => "osd1_applied is reserved",
        DENY_DURABILITY => "pragma is not allowed",
        _ => "not authorized",
    }
}

fn is_durability_pragma(name: &str) -> bool {
    const NAMES: &[&str] = &[
        "synchronous",
        "journal_mode",
        "wal_autocheckpoint",
        "locking_mode",
        "mmap_size",
        "page_size",
    ];
    NAMES.iter().any(|item| name.eq_ignore_ascii_case(item))
}

fn touches_applied_table(action: &AuthAction<'_>) -> bool {
    let name = match action {
        AuthAction::Read { table_name, .. }
        | AuthAction::Insert { table_name, .. }
        | AuthAction::Update { table_name, .. }
        | AuthAction::Delete { table_name, .. }
        | AuthAction::DropTable { table_name, .. }
        | AuthAction::AlterTable { table_name, .. }
        | AuthAction::CreateIndex { table_name, .. } => table_name,
        _ => return false,
    };
    name.eq_ignore_ascii_case(APPLIED_TABLE)
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
    Ok(SqlResponse {
        statements,
        commit_index: None,
    })
}

fn max_applied_origin(
    conn: &Connection,
    internal: &AtomicBool,
    origin: u64,
) -> Result<u64, SqlError> {
    let exists = conn
        .prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'osd1_applied'")
        .and_then(|mut statement| statement.exists([]))
        .map_err(|error| SqlError::Sqlite {
            code: 1,
            message: error.to_string(),
        })?;
    if !exists {
        return Ok(0);
    }
    let id = allow_internal(internal, || {
        conn.query_row(
            "SELECT COALESCE(MAX(id), 0) FROM osd1_applied WHERE origin = ?1",
            [origin as i64],
            |row| row.get::<_, i64>(0),
        )
    })
    .map_err(|error| SqlError::Sqlite {
        code: 1,
        message: error.to_string(),
    })?;
    Ok(id.max(0) as u64)
}

fn execute_idempotent(
    conn: &Connection,
    path: &Path,
    colo: &Arc<str>,
    region: &Arc<str>,
    size_after: &mut u64,
    column_cache: &mut ColumnCache,
    internal: &AtomicBool,
    schema_changed: &AtomicBool,
    deny_code: &AtomicU8,
    origin: u64,
    id: u64,
    log_index: u64,
    request: SqlRequest,
    applied_at: &mut HashMap<u64, Instant>,
) -> Result<SqlResponse, SqlError> {
    deny_code.store(DENY_NONE, Ordering::Relaxed);
    exec_cached(conn, "BEGIN IMMEDIATE", internal, deny_code)?;
    if let Err(error) = ensure_applied_table(conn, internal) {
        rollback(conn, internal, deny_code);
        return Err(map_sqlite_error(error, deny_code));
    }
    let reserved = allow_internal(internal, || {
        let idx: Option<i64> = (log_index > 0).then_some(log_index as i64);
        conn.execute(
            "INSERT OR IGNORE INTO osd1_applied (origin, id, idx) VALUES (?1, ?2, ?3)",
            rusqlite::params![origin as i64, id as i64, idx],
        )
    });
    let inserted = match reserved {
        Ok(inserted) => inserted,
        Err(error) => {
            rollback(conn, internal, deny_code);
            return Err(map_sqlite_error(error, deny_code));
        }
    };
    if inserted == 0 {
        let stored = allow_internal(internal, || {
            conn.query_row(
                "SELECT body FROM osd1_applied WHERE origin = ?1 AND id = ?2",
                [origin as i64, id as i64],
                |row| row.get::<_, Option<String>>(0),
            )
        });
        let stored = match stored {
            Ok(body) => body,
            Err(error) => {
                rollback(conn, internal, deny_code);
                return Err(map_sqlite_error(error, deny_code));
            }
        };
        rollback(conn, internal, deny_code);
        return stored_response(stored);
    }
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
    let measured = if statements.iter().any(|statement| statement.meta.changed_db) {
        fs::metadata(path)
            .map(|meta| meta.len())
            .unwrap_or(*size_after)
    } else {
        *size_after
    };
    let mut statements = statements;
    for statement in &mut statements {
        statement.meta.size_after = measured;
    }
    let response = SqlResponse {
        statements,
        commit_index: None,
    };
    let body = match response.to_stored() {
        Ok(body) => body,
        Err(error) => {
            rollback(conn, internal, deny_code);
            return Err(SqlError::Sqlite {
                code: 1,
                message: error,
            });
        }
    };
    let saved = allow_internal(internal, || {
        conn.execute(
            "UPDATE osd1_applied SET body = ?1 WHERE origin = ?2 AND id = ?3",
            rusqlite::params![body, origin as i64, id as i64],
        )
    });
    if let Err(error) = saved {
        rollback(conn, internal, deny_code);
        return Err(map_sqlite_error(error, deny_code));
    }
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
    *size_after = measured;
    if log_index > 0 {
        applied_at.insert(log_index, Instant::now());
    }
    Ok(response)
}

fn ensure_applied_table(conn: &Connection, internal: &AtomicBool) -> Result<(), rusqlite::Error> {
    allow_internal(internal, || {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS osd1_applied (
                origin INTEGER NOT NULL,
                id INTEGER NOT NULL,
                body TEXT,
                idx INTEGER,
                PRIMARY KEY (origin, id)
            )",
        )?;
        let mut info = conn.prepare("PRAGMA table_info(osd1_applied)")?;
        let names = info
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?;
        drop(info);
        if !names.iter().any(|name| name.eq_ignore_ascii_case("body")) {
            conn.execute_batch("ALTER TABLE osd1_applied ADD COLUMN body TEXT")?;
        }
        if !names.iter().any(|name| name.eq_ignore_ascii_case("idx")) {
            conn.execute_batch("ALTER TABLE osd1_applied ADD COLUMN idx INTEGER")?;
        }
        Ok(())
    })
}

fn clear_applied_bodies(
    conn: &Connection,
    internal: &AtomicBool,
    index: u64,
    now: Instant,
    applied_at: &mut HashMap<u64, Instant>,
) -> Result<(), SqlError> {
    if !conn.is_autocommit() {
        return Ok(());
    }
    ensure_applied_table(conn, internal).map_err(|error| SqlError::Sqlite {
        code: 1,
        message: error.to_string(),
    })?;
    let mut cutoff = index;
    for (idx, at) in applied_at.iter() {
        if *idx <= index && now.saturating_duration_since(*at) < APPLIED_BODY_HOLD {
            cutoff = cutoff.min(idx.saturating_sub(1));
        }
    }
    let cleared = allow_internal(internal, || {
        conn.execute(
            "UPDATE osd1_applied SET body = NULL
             WHERE idx IS NOT NULL AND idx <= ?1 AND body IS NOT NULL",
            [cutoff as i64],
        )
    })
    .map(|_| ())
    .map_err(|error| SqlError::Sqlite {
        code: 1,
        message: error.to_string(),
    });
    if cleared.is_ok() {
        applied_at.retain(|idx, at| {
            *idx > cutoff && now.saturating_duration_since(*at) < APPLIED_BODY_HOLD
        });
    }
    cleared
}

fn stored_response(body: Option<String>) -> Result<SqlResponse, SqlError> {
    let Some(body) = body.filter(|body| !body.is_empty()) else {
        return Ok(SqlResponse {
            statements: Vec::new(),
            commit_index: None,
        });
    };
    SqlResponse::from_stored(&body).map_err(|error| SqlError::Sqlite {
        code: 1,
        message: format!("stored result: {error}"),
    })
}

fn backup_database(conn: &Connection, dest: &Path) -> Result<(), SqlError> {
    if let Some(parent) = dest
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        let _ = fs::create_dir_all(parent);
    }
    let _ = fs::remove_file(dest);
    conn.backup(rusqlite::DatabaseName::Main, dest, None)
        .map_err(|error| map_sqlite_error(error, &AtomicU8::new(DENY_NONE)))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(dest, fs::Permissions::from_mode(0o600));
    }
    Ok(())
}

fn snapshot_bytes(conn: &Connection, path: &Path) -> Result<Vec<u8>, SqlError> {
    let snap = path.with_file_name(format!(
        ".{}.snap",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("osd1.db")
    ));
    backup_database(conn, &snap)?;
    let bytes = fs::read(&snap).map_err(|error| SqlError::Sqlite {
        code: 1,
        message: error.to_string(),
    });
    let _ = fs::remove_file(&snap);
    bytes
}

fn replace_database_file(path: &Path, bytes: &[u8]) -> Result<(), SqlError> {
    let tmp = path.with_file_name(format!(
        ".{}.install",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("osd1.db")
    ));
    {
        use std::io::Write;
        let mut file = fs::File::create(&tmp).map_err(|error| SqlError::Sqlite {
            code: 1,
            message: error.to_string(),
        })?;
        file.write_all(bytes).map_err(|error| SqlError::Sqlite {
            code: 1,
            message: error.to_string(),
        })?;
        file.sync_all().map_err(|error| SqlError::Sqlite {
            code: 1,
            message: error.to_string(),
        })?;
    }
    for suffix in ["-wal", "-shm"] {
        let sidecar = PathBuf::from(format!("{}{suffix}", path.display()));
        let _ = fs::remove_file(sidecar);
    }
    fs::rename(&tmp, path).map_err(|error| SqlError::Sqlite {
        code: 1,
        message: error.to_string(),
    })?;
    restrict_database_files(path);
    if let Some(dir) = path.parent() {
        if let Ok(dir_file) = fs::File::open(dir) {
            let _ = dir_file.sync_all();
        }
    }
    Ok(())
}

fn reopen_connection(
    path: &Path,
    internal: &Arc<AtomicBool>,
    schema_changed: &Arc<AtomicBool>,
    deny_code: &Arc<AtomicU8>,
    deny_nondeterministic: &Arc<AtomicBool>,
) -> Result<Connection, SqlError> {
    let conn = Connection::open(path).map_err(|error| SqlError::Sqlite {
        code: 1,
        message: error.to_string(),
    })?;
    prepare_connection(&conn).map_err(|error| SqlError::Sqlite {
        code: 1,
        message: error.to_string(),
    })?;
    install_authorizer(
        &conn,
        Arc::clone(internal),
        Arc::clone(schema_changed),
        Arc::clone(deny_code),
        Arc::clone(deny_nondeterministic),
    )
    .map_err(|error| SqlError::Sqlite {
        code: 1,
        message: error.to_string(),
    })?;
    install_progress(&conn);
    conn.set_prepared_statement_cache_capacity(32);
    restrict_database_files(path);
    Ok(conn)
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
    Ok(SqlResponse {
        statements,
        commit_index: None,
    })
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
    if code & 0xff == rusqlite::ffi::SQLITE_INTERRUPT {
        let expired = SQL_DEADLINE.with(|cell| cell.get().is_some_and(|at| Instant::now() >= at));
        if expired {
            SQL_DEADLINE.with(|cell| cell.set(None));
            return SqlError::TimedOut;
        }
    }
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
mod fuzz;

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
            deadline: None,
            min_commit_index: None,
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
            deadline: None,
            min_commit_index: None,
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
        run(&worker, "ALTER TABLE t DROP COLUMN m", vec![]).unwrap();
        let dropped = run(&worker, "SELECT * FROM t", vec![]).unwrap();
        assert_eq!(
            dropped.statements[0].columns.as_ref(),
            ["n".to_string()].as_slice()
        );
        assert_eq!(dropped.statements[0].rows, vec![vec![SqlValue::Integer(1)]]);
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
    fn durability_pragmas_are_denied() {
        let path = temp_db("durability");
        let worker = SqliteWorker::open(&path, 4, "LOC").unwrap();
        for sql in [
            "PRAGMA synchronous=OFF",
            "PRAGMA journal_mode=DELETE",
            "PRAGMA wal_autocheckpoint=0",
            "PRAGMA locking_mode=EXCLUSIVE",
            "PRAGMA mmap_size=0",
            "PRAGMA page_size=8192",
        ] {
            let error = run(&worker, sql, vec![]).unwrap_err();
            assert!(
                matches!(error, SqlError::AuthorizerDenied { ref message } if message == "pragma is not allowed"),
                "{sql} -> {error:?}"
            );
        }
        run(&worker, "PRAGMA integrity_check", vec![]).unwrap();
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
    fn describe_table_returns_definition_columns_and_indexes() {
        let path = temp_db("describe");
        let worker = SqliteWorker::open(&path, 4, "LOC").unwrap();
        run(
            &worker,
            "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            vec![],
        )
        .unwrap();
        run(&worker, "CREATE INDEX items_name ON items (name)", vec![]).unwrap();
        let response = run(
            &worker,
            "SELECT type, name, sql FROM sqlite_master WHERE name = ?1 AND type IN ('table', 'view');\nSELECT cid, name, type, \"notnull\", dflt_value, pk FROM pragma_table_info(?1) ORDER BY cid;\nSELECT name, \"unique\", origin, partial FROM pragma_index_list(?1) ORDER BY seq",
            vec![SqlValue::Text("items".to_string())],
        )
        .unwrap();
        assert_eq!(response.statements.len(), 3);
        assert_eq!(
            response.statements[0].rows[0][0],
            SqlValue::Text("table".to_string())
        );
        assert_eq!(
            response.statements[0].rows[0][1],
            SqlValue::Text("items".to_string())
        );
        match &response.statements[0].rows[0][2] {
            SqlValue::Text(sql) => assert!(sql.contains("name TEXT")),
            other => panic!("create text {other:?}"),
        }
        assert_eq!(
            response.statements[1].rows[0][1],
            SqlValue::Text("id".to_string())
        );
        assert_eq!(response.statements[1].rows[0][5], SqlValue::Integer(1));
        assert_eq!(
            response.statements[1].rows[1][1],
            SqlValue::Text("name".to_string())
        );
        assert_eq!(response.statements[1].rows[1][3], SqlValue::Integer(1));
        assert_eq!(
            response.statements[2].rows[0][0],
            SqlValue::Text("items_name".to_string())
        );
        assert_eq!(response.statements[2].rows[0][1], SqlValue::Integer(0));

        let missing = run(
            &worker,
            "SELECT type, name, sql FROM sqlite_master WHERE name = ?1 AND type IN ('table', 'view');\nSELECT cid, name, type, \"notnull\", dflt_value, pk FROM pragma_table_info(?1) ORDER BY cid;\nSELECT name, \"unique\", origin, partial FROM pragma_index_list(?1) ORDER BY seq",
            vec![SqlValue::Text("absent".to_string())],
        )
        .unwrap();
        assert!(missing
            .statements
            .iter()
            .all(|statement| statement.rows.is_empty()));
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
            deadline: None,
            min_commit_index: None,
        }
    }

    fn enqueue(worker: &SqliteWorker, sql: &str, params: Vec<SqlValue>) -> EnqueueResult {
        worker.enqueue(SqlRequest {
            sql: sql.to_string(),
            params,
            shape: ResultShape::Objects,
            deadline: None,
            min_commit_index: None,
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

    #[test]
    fn a_repeated_command_id_does_not_insert_again_and_a_snapshot_restores_the_file() {
        let path = temp_db("applied");
        let worker = SqliteWorker::open(&path, 4, "LOC").unwrap();
        assert_eq!(worker.max_applied_id(1).expect("floor"), 0);
        let request = SqlRequest {
            sql: "CREATE TABLE t(n INTEGER); INSERT INTO t(n) VALUES (4)".to_string(),
            params: vec![],
            shape: ResultShape::Objects,
            deadline: None,
            min_commit_index: None,
        };
        let first = worker
            .execute_idempotent(1, 7, 0, request.clone())
            .expect("apply");
        let replay = worker.execute_idempotent(1, 7, 0, request).expect("replay");
        let stored = SqlResponse::from_stored(&first.to_stored().expect("json")).expect("stored");
        assert_eq!(replay.statements, stored.statements);
        assert!(!first.statements.is_empty());
        assert_eq!(worker.max_applied_id(1).expect("floor"), 7);
        assert_eq!(worker.max_applied_id(2).expect("other origin"), 0);
        let rows = run(&worker, "SELECT n FROM t", vec![]).unwrap();
        assert_eq!(rows.statements[0].rows, vec![vec![SqlValue::Integer(4)]]);
        let denied = run(&worker, "SELECT origin FROM osd1_applied", vec![]);
        assert!(denied.is_err(), "{denied:?}");

        let bytes = worker.snapshot_main().expect("snapshot");
        let copy = temp_db("applied-copy");
        let restored = SqliteWorker::open(&copy, 4, "LOC").unwrap();
        restored.install_main(bytes).expect("install");
        let rows = run(&restored, "SELECT n FROM t", vec![]).unwrap();
        assert_eq!(rows.statements[0].rows, vec![vec![SqlValue::Integer(4)]]);
    }

    #[test]
    fn replace_database_file_drops_the_wal_and_keeps_the_new_bytes() {
        let path = temp_db("install-bytes");
        fs::write(&path, b"old-database").unwrap();
        let wal = PathBuf::from(format!("{}-wal", path.display()));
        fs::write(&wal, b"wal-sidecar").unwrap();
        let bytes = b"new-database-bytes".to_vec();
        replace_database_file(&path, &bytes).expect("install");
        assert!(!wal.exists(), "wal sidecar was not removed");
        assert_eq!(fs::read(&path).expect("database"), bytes);
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn a_deadline_interrupts_sql_and_the_next_statement_runs() {
        let path = temp_db("deadline");
        let worker = SqliteWorker::open(&path, 4, "LOC").unwrap();
        let mut request = sql_request(
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 10000000) SELECT max(x) FROM c",
        );
        request.deadline = Some(Instant::now() + std::time::Duration::from_millis(50));
        let started = Instant::now();
        let error = match worker.enqueue(request) {
            EnqueueResult::Accepted(receiver) => {
                receiver.blocking_recv().expect("reply").unwrap_err()
            }
            EnqueueResult::Full => panic!("queue full"),
            EnqueueResult::Closed => panic!("worker closed"),
        };
        assert!(matches!(error, SqlError::TimedOut), "{error:?}");
        assert!(
            started.elapsed() < std::time::Duration::from_secs(2),
            "the statement was not interrupted"
        );
        run(&worker, "SELECT 1", vec![]).unwrap();

        let mut expired = sql_request("SELECT * FROM not_a_table");
        expired.deadline = Some(Instant::now() - std::time::Duration::from_millis(5));
        let error = match worker.enqueue(expired) {
            EnqueueResult::Accepted(receiver) => {
                receiver.blocking_recv().expect("reply").unwrap_err()
            }
            EnqueueResult::Full => panic!("queue full"),
            EnqueueResult::Closed => panic!("worker closed"),
        };
        assert!(matches!(error, SqlError::TimedOut), "{error:?}");
    }

    #[test]
    fn an_old_applied_table_keeps_its_rows_and_stores_new_results() {
        let path = temp_db("applied-migrate");
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE osd1_applied (
                    origin INTEGER NOT NULL,
                    id INTEGER NOT NULL,
                    PRIMARY KEY (origin, id)
                );
                INSERT INTO osd1_applied (origin, id) VALUES (1, 3);",
            )
            .unwrap();
        }
        let worker = SqliteWorker::open(&path, 4, "LOC").unwrap();
        let old = worker
            .execute_idempotent(1, 3, 0, sql_request("CREATE TABLE skipped(n INTEGER)"))
            .unwrap();
        assert!(old.statements.is_empty());
        let request = sql_request("CREATE TABLE t(n INTEGER); INSERT INTO t(n) VALUES (9)");
        let first = worker
            .execute_idempotent(1, 4, 0, request.clone())
            .expect("apply");
        let second = worker.execute_idempotent(1, 4, 0, request).expect("replay");
        let stored = SqlResponse::from_stored(&first.to_stored().expect("json")).expect("stored");
        assert_eq!(second.statements, stored.statements);
        assert!(!first.statements.is_empty());
        let rows = run(&worker, "SELECT n FROM t", vec![]).unwrap();
        assert_eq!(rows.statements[0].rows, vec![vec![SqlValue::Integer(9)]]);
        let skipped = run(
            &worker,
            "SELECT name FROM sqlite_master WHERE name = 'skipped'",
            vec![],
        )
        .unwrap();
        assert!(skipped.statements[0].rows.is_empty());
    }

    #[test]
    fn a_purged_index_clears_the_stored_body_and_keeps_the_row() {
        let path = temp_db("horizon");
        let worker = SqliteWorker::open(&path, 4, "LOC").unwrap();
        worker
            .execute_idempotent(1, 1, 4, sql_request("CREATE TABLE t(n INTEGER)"))
            .unwrap();
        let insert = sql_request("INSERT INTO t(n) VALUES (1)");
        let stored = worker.execute_idempotent(1, 2, 8, insert.clone()).unwrap();
        let stored = SqlResponse::from_stored(&stored.to_stored().expect("json")).expect("stored");
        assert!(!stored.statements.is_empty());
        let unknown = sql_request("INSERT INTO t(n) VALUES (2)");
        let unknown_stored = worker.execute_idempotent(1, 3, 0, unknown.clone()).unwrap();
        let unknown_stored =
            SqlResponse::from_stored(&unknown_stored.to_stored().expect("json")).expect("stored");
        assert!(!unknown_stored.statements.is_empty());

        worker.forget_applied_bodies(4).unwrap();
        let kept_early = worker
            .execute_idempotent(1, 1, 4, sql_request("CREATE TABLE t(n INTEGER)"))
            .unwrap();
        assert!(
            !kept_early.statements.is_empty(),
            "a body applied moments ago stays through the purge"
        );
        let later = std::time::Instant::now() + std::time::Duration::from_secs(4);
        worker.forget_applied_bodies_at(4, later).unwrap();
        let replay_old = worker
            .execute_idempotent(1, 1, 4, sql_request("CREATE TABLE t(n INTEGER)"))
            .unwrap();
        assert!(replay_old.statements.is_empty());
        let replay_kept = worker.execute_idempotent(1, 2, 8, insert).unwrap();
        assert_eq!(replay_kept.statements, stored.statements);
        let replay_unknown = worker.execute_idempotent(1, 3, 0, unknown).unwrap();
        assert_eq!(replay_unknown.statements, unknown_stored.statements);
        let rows = run(&worker, "SELECT COUNT(*) AS c FROM t", vec![]).unwrap();
        assert_eq!(rows.statements[0].rows, vec![vec![SqlValue::Integer(2)]]);

        worker.forget_applied_bodies_at(8, later).unwrap();
        let cleared = worker
            .execute_idempotent(1, 2, 8, sql_request("INSERT INTO t(n) VALUES (1)"))
            .unwrap();
        assert!(cleared.statements.is_empty());
        let still = worker
            .execute_idempotent(1, 3, 0, sql_request("INSERT INTO t(n) VALUES (2)"))
            .unwrap();
        assert_eq!(still.statements, unknown_stored.statements);
        let rows = run(&worker, "SELECT COUNT(*) AS c FROM t", vec![]).unwrap();
        assert_eq!(rows.statements[0].rows, vec![vec![SqlValue::Integer(2)]]);
    }

    #[test]
    fn fts5_match_returns_the_row_text() {
        let path = temp_db("fts5");
        let worker = SqliteWorker::open(&path, 4, "LOC").unwrap();
        run(
            &worker,
            "CREATE VIRTUAL TABLE docs USING fts5(body)",
            vec![],
        )
        .unwrap();
        run(
            &worker,
            "INSERT INTO docs(rowid, body) VALUES (?, ?)",
            vec![SqlValue::Integer(1), SqlValue::Text("red apple".into())],
        )
        .unwrap();
        run(
            &worker,
            "INSERT INTO docs(rowid, body) VALUES (?, ?)",
            vec![SqlValue::Integer(2), SqlValue::Text("green leaf".into())],
        )
        .unwrap();
        let response = run(
            &worker,
            "SELECT rowid, body FROM docs WHERE docs MATCH ?",
            vec![SqlValue::Text("apple".into())],
        )
        .unwrap();
        assert_eq!(
            response.statements[0].rows,
            vec![vec![
                SqlValue::Integer(1),
                SqlValue::Text("red apple".into())
            ]]
        );
    }

    #[test]
    fn json_vector_scan_returns_distance_and_text() {
        let path = temp_db("vector");
        let worker = SqliteWorker::open(&path, 4, "LOC").unwrap();
        run(
            &worker,
            "CREATE TABLE notes (
                id INTEGER PRIMARY KEY,
                body TEXT NOT NULL,
                embedding TEXT NOT NULL
            )",
            vec![],
        )
        .unwrap();
        run(
            &worker,
            "INSERT INTO notes (id, body, embedding) VALUES (?, ?, ?)",
            vec![
                SqlValue::Integer(1),
                SqlValue::Text("red apple".into()),
                SqlValue::Text("[1,0,0]".into()),
            ],
        )
        .unwrap();
        run(
            &worker,
            "INSERT INTO notes (id, body, embedding) VALUES (?, ?, ?)",
            vec![
                SqlValue::Integer(2),
                SqlValue::Text("green leaf".into()),
                SqlValue::Text("[0,1,0]".into()),
            ],
        )
        .unwrap();
        let response = run(
            &worker,
            "SELECT body, (
                SELECT sum((a.value - b.value) * (a.value - b.value))
                FROM json_each(notes.embedding) AS a
                JOIN json_each(?) AS b ON a.key = b.key
             ) AS distance
             FROM notes
             ORDER BY distance, id",
            vec![SqlValue::Text("[1,0,0]".into())],
        )
        .unwrap();
        let rows = &response.statements[0].rows;
        assert_eq!(rows.len(), 2, "{rows:?}");
        assert_eq!(rows[0][0], SqlValue::Text("red apple".into()));
        assert_eq!(rows[1][0], SqlValue::Text("green leaf".into()));
        assert!(is_zero(&rows[0][1]), "{rows:?}");
        assert!(is_two(&rows[1][1]), "{rows:?}");
    }

    #[test]
    fn vec0_match_returns_row_id_distance_and_text() {
        let path = temp_db("vec0");
        let worker = SqliteWorker::open(&path, 4, "LOC").unwrap();
        run(
            &worker,
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
            vec![],
        )
        .unwrap();
        run(
            &worker,
            "CREATE VIRTUAL TABLE notes_vec USING vec0(embedding float[3])",
            vec![],
        )
        .unwrap();
        run(
            &worker,
            "INSERT INTO notes (id, body) VALUES (1, 'red apple')",
            vec![],
        )
        .unwrap();
        run(
            &worker,
            "INSERT INTO notes_vec (rowid, embedding) VALUES (1, '[1,0,0]')",
            vec![],
        )
        .unwrap();
        run(
            &worker,
            "INSERT INTO notes (id, body) VALUES (2, 'green leaf')",
            vec![],
        )
        .unwrap();
        run(
            &worker,
            "INSERT INTO notes_vec (rowid, embedding) VALUES (2, '[0,1,0]')",
            vec![],
        )
        .unwrap();
        let response = run(
            &worker,
            "SELECT notes.id, notes.body, notes_vec.distance
             FROM notes_vec
             JOIN notes ON notes.id = notes_vec.rowid
             WHERE notes_vec.embedding MATCH ?
               AND k = 2
             ORDER BY notes_vec.distance",
            vec![SqlValue::Text("[1,0,0]".into())],
        )
        .unwrap();
        let rows = &response.statements[0].rows;
        assert_eq!(rows.len(), 2, "{rows:?}");
        assert_eq!(rows[0][0], SqlValue::Integer(1));
        assert_eq!(rows[0][1], SqlValue::Text("red apple".into()));
        assert!(is_zero(&rows[0][2]), "{rows:?}");
        assert_eq!(rows[1][0], SqlValue::Integer(2));
        assert_eq!(rows[1][1], SqlValue::Text("green leaf".into()));
        assert!(!is_zero(&rows[1][2]), "{rows:?}");
    }

    fn is_zero(value: &SqlValue) -> bool {
        match value {
            SqlValue::Integer(0) => true,
            SqlValue::Real(real) => *real == 0.0,
            _ => false,
        }
    }

    fn is_two(value: &SqlValue) -> bool {
        match value {
            SqlValue::Integer(2) => true,
            SqlValue::Real(real) => *real == 2.0,
            _ => false,
        }
    }
}
