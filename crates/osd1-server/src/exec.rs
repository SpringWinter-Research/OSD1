//! [`SqlExecutor`] for a raft node.
//!
//! `enqueue` returns a oneshot and does the raft work on a spawned task.
//! Simple mode does not construct this type, so it never opens a log.

use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use osd1_engine::SqliteWorker;
use osd1_raft::{client_write, ProposeError, RaftNode, SqlCommand};
use osd1_types::{EnqueueResult, SqlError, SqlExecutor, SqlRequest, SqlResponse};

use crate::sql_error;
use crate::wire::{decode_body, params_json};

/// Next command id for this process. The file is the value that will be issued next.
///
/// It is advanced and synced before the id is proposed, so a crash cannot reuse an id
/// that might already be in the log. `(origin, id)` is applied once.
pub(crate) struct CommandIds {
    path: PathBuf,
    next: std::sync::Mutex<u64>,
}

impl CommandIds {
    /// `floor` is one past the highest id already stored for this origin.
    pub(crate) fn load(dir: &Path, floor: u64) -> std::io::Result<Self> {
        let path = dir.join("next-command-id");
        let stored = read_u64(&path)?;
        let next = stored.unwrap_or(0).max(floor).max(1);
        if stored != Some(next) {
            write_u64(&path, next)?;
        }
        Ok(Self {
            path,
            next: std::sync::Mutex::new(next),
        })
    }

    fn allocate(&self) -> std::io::Result<u64> {
        let mut next = self.next.lock().expect("command ids");
        let id = *next;
        let ahead = id.saturating_add(1);
        write_u64(&self.path, ahead)?;
        *next = ahead;
        Ok(id)
    }
}

#[derive(Clone)]
pub(crate) struct RaftExec {
    worker: Arc<SqliteWorker>,
    node: RaftNode,
    secret: Arc<str>,
    raft_addr: String,
    ids: Arc<CommandIds>,
    /// Bounds a local read. A write waits for raft instead, so a commit is not
    /// reported as a timeout and then applied under a second command id.
    request_timeout: Option<Duration>,
}

impl RaftExec {
    pub(crate) fn new(
        worker: Arc<SqliteWorker>,
        node: RaftNode,
        secret: Arc<str>,
        raft_addr: String,
        ids: CommandIds,
        request_timeout: Option<Duration>,
    ) -> Self {
        Self {
            worker,
            node,
            secret,
            raft_addr,
            ids: Arc::new(ids),
            request_timeout,
        }
    }

    async fn dispatch(&self, request: SqlRequest) -> Result<SqlResponse, SqlError> {
        // `writes` only prepares. A full queue here is the same 429 as enqueue.
        match self.statement_writes(&request).await {
            Ok(true) => self.write(request).await,
            Ok(false) => self.bounded(self.read(request)).await,
            Err(error) => Err(error),
        }
    }

    async fn bounded(
        &self,
        pending: impl std::future::Future<Output = Result<SqlResponse, SqlError>>,
    ) -> Result<SqlResponse, SqlError> {
        match self.request_timeout {
            Some(limit) => match tokio::time::timeout(limit, pending).await {
                Ok(result) => result,
                Err(_elapsed) => Err(SqlError::TimedOut),
            },
            None => pending.await,
        }
    }

    async fn statement_writes(&self, request: &SqlRequest) -> Result<bool, SqlError> {
        let worker = Arc::clone(&self.worker);
        let request = request.clone();
        tokio::task::spawn_blocking(move || worker.writes(request))
            .await
            .map_err(|_| SqlError::ShuttingDown)?
    }

    async fn read(&self, mut request: SqlRequest) -> Result<SqlResponse, SqlError> {
        if let Some(limit) = self.request_timeout {
            request.deadline = Some(std::time::Instant::now() + limit);
        }
        if let Some(target) = request.min_commit_index {
            self.wait_applied(target, request.deadline).await?;
        }
        // The sample is the metrics watch, so a busy raft core does not hold the read.
        let leader = self.node.is_leader();
        let enqueued = if leader {
            self.worker.enqueue(request)
        } else {
            self.worker.enqueue_read(request)
        };
        let mut response = match enqueued {
            EnqueueResult::Accepted(receiver) => {
                receiver.await.unwrap_or(Err(SqlError::ShuttingDown))?
            }
            EnqueueResult::Full => return Err(SqlError::QueueFull),
            EnqueueResult::Closed => return Err(SqlError::ShuttingDown),
        };
        // Leadership can move while the read sits in the SQL queue.
        if leader && !self.node.is_leader() {
            for statement in &mut response.statements {
                statement.meta.served_by_primary = false;
            }
        }
        Ok(response)
    }

    async fn write(&self, request: SqlRequest) -> Result<SqlResponse, SqlError> {
        let ids = Arc::clone(&self.ids);
        let id = tokio::task::spawn_blocking(move || ids.allocate())
            .await
            .map_err(|_| SqlError::ShuttingDown)?
            .map_err(|error| sql_error(format!("command id: {error}")))?;
        let command = SqlCommand {
            origin: self.node.id(),
            id,
            sql: request.sql,
            params_json: params_json(&request.params)?,
            log_index: 0,
        };
        // One id for this HTTP request. A timed-out forward retries that id
        // so a commit that already happened is not applied twice.
        if self.node.is_leader() {
            match self.node.propose(command.clone()).await {
                Ok(response) => {
                    return attach_commit(decode_body(&response.body_json)?, response.commit_index)
                }
                Err(ProposeError::NotLeader { .. }) => {}
                Err(ProposeError::Rejected(message)) => return Err(sql_error(message)),
            }
        }
        self.forward(&command).await
    }

    async fn forward(&self, command: &SqlCommand) -> Result<SqlResponse, SqlError> {
        let mut last = "no raft leader".to_string();
        // One election is 150–300 ms, and a paused leader took about 2 s to
        // give up the term. Keep this command id across that window.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while tokio::time::Instant::now() < deadline {
            let Some(advertise) = self.node.leader_advertise().await else {
                last = "no raft leader".to_string();
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            };
            let addr = raft_half(&advertise).to_string();
            if addr == self.raft_addr {
                match self.node.propose(command.clone()).await {
                    Ok(response) => {
                        return attach_commit(
                            decode_body(&response.body_json)?,
                            response.commit_index,
                        )
                    }
                    Err(ProposeError::NotLeader { .. }) => last = "not the leader".to_string(),
                    Err(ProposeError::Rejected(message)) => return Err(sql_error(message)),
                }
            } else {
                match client_write(&addr, &self.secret, command).await {
                    Ok(reply) if reply.ok => {
                        return attach_commit(decode_body(&reply.body_json)?, reply.commit_index)
                    }
                    Ok(reply) => {
                        last = if reply.error.is_empty() {
                            "not the leader".to_string()
                        } else {
                            reply.error
                        };
                    }
                    Err(error) => last = error,
                }
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        Err(sql_error(format!(
            "raft leader did not accept the write: {last}"
        )))
    }

    /// Wait until this node's apply index reaches `target`. The wait sits in
    /// front of the SQL queue and ends at the read deadline. The index comes
    /// from the metrics watch, so the wait does not queue behind an append.
    async fn wait_applied(
        &self,
        target: u64,
        deadline: Option<std::time::Instant>,
    ) -> Result<(), SqlError> {
        loop {
            if self.node.applied_index() >= target {
                return Ok(());
            }
            if deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
                return Err(SqlError::TimedOut);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

fn attach_commit(mut response: SqlResponse, index: u64) -> Result<SqlResponse, SqlError> {
    if index > 0 {
        response.commit_index = Some(index);
    }
    Ok(response)
}

impl SqlExecutor for RaftExec {
    fn enqueue(&self, request: SqlRequest) -> EnqueueResult {
        if self.worker.sql_inflight() > self.worker.sql_queue_capacity() {
            return EnqueueResult::Full;
        }
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let exec = self.clone();
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return EnqueueResult::Closed;
        };
        handle.spawn(async move {
            let result = exec.dispatch(request).await;
            let _ = sender.send(result);
        });
        EnqueueResult::Accepted(receiver)
    }

    fn sql_inflight(&self) -> usize {
        self.worker.sql_inflight()
    }

    fn sql_queue_capacity(&self) -> usize {
        self.worker.sql_queue_capacity()
    }
}

fn raft_half(addr: &str) -> &str {
    addr.split('|').next().unwrap_or(addr)
}

fn read_u64(path: &Path) -> std::io::Result<Option<u64>> {
    if !path.exists() {
        return Ok(None);
    }
    let text = fs::read_to_string(path)?;
    let value = text
        .trim()
        .parse::<u64>()
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    Ok(Some(value))
}

fn write_u64(path: &Path, value: u64) -> std::io::Result<()> {
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("next-command-id");
    let tmp = dir.join(format!(".{name}.tmp"));
    let mut file = File::create(&tmp)?;
    file.write_all(format!("{value}\n").as_bytes())?;
    file.sync_all()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(&tmp, fs::Permissions::from_mode(0o600));
    }
    fs::rename(&tmp, path)?;
    if let Ok(dir_file) = File::open(dir) {
        let _ = dir_file.sync_all();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_ids_survive_a_new_process_and_skip_ids_already_in_the_log() {
        let dir = std::env::temp_dir().join(format!(
            "osd1-command-ids-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("dir");

        let ids = CommandIds::load(&dir, 1).expect("load");
        assert_eq!(ids.allocate().expect("id"), 1);
        assert_eq!(ids.allocate().expect("id"), 2);
        drop(ids);

        let ids = CommandIds::load(&dir, 1).expect("reload");
        assert_eq!(ids.allocate().expect("id after restart"), 3);
        drop(ids);

        let ids = CommandIds::load(&dir, 10).expect("floor");
        assert_eq!(ids.allocate().expect("id past the log"), 10);
        let _ = fs::remove_dir_all(&dir);
    }
}
