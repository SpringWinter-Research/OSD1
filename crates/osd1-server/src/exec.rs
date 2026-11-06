//! [`SqlExecutor`] for a raft node.
//!
//! `enqueue` returns a oneshot and does the raft work on a spawned task.
//! Simple mode does not construct this type, so it never opens a log.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use osd1_engine::SqliteWorker;
use osd1_raft::{client_write, ProposeError, RaftNode, SqlCommand};
use osd1_types::{EnqueueResult, SqlError, SqlExecutor, SqlRequest, SqlResponse};

use crate::sql_error;
use crate::wire::{decode_body, params_json};

#[derive(Clone)]
pub(crate) struct RaftExec {
    worker: Arc<SqliteWorker>,
    node: RaftNode,
    secret: Arc<str>,
    raft_addr: String,
    ids: Arc<AtomicU64>,
}

impl RaftExec {
    pub(crate) fn new(
        worker: Arc<SqliteWorker>,
        node: RaftNode,
        secret: Arc<str>,
        raft_addr: String,
    ) -> Self {
        Self {
            worker,
            node,
            secret,
            raft_addr,
            ids: Arc::new(AtomicU64::new(1)),
        }
    }

    async fn dispatch(&self, request: SqlRequest) -> Result<SqlResponse, SqlError> {
        // `writes` only prepares. A full queue here is the same 429 as enqueue.
        match self.statement_writes(&request).await {
            Ok(true) => self.write(request).await,
            Ok(false) => self.read(request).await,
            Err(error) => Err(error),
        }
    }

    async fn statement_writes(&self, request: &SqlRequest) -> Result<bool, SqlError> {
        let worker = Arc::clone(&self.worker);
        let request = request.clone();
        tokio::task::spawn_blocking(move || worker.writes(request))
            .await
            .map_err(|_| SqlError::ShuttingDown)?
    }

    async fn read(&self, request: SqlRequest) -> Result<SqlResponse, SqlError> {
        let enqueued = if self.is_leader().await {
            self.worker.enqueue(request)
        } else {
            self.worker.enqueue_read(request)
        };
        match enqueued {
            EnqueueResult::Accepted(receiver) => {
                receiver.await.unwrap_or(Err(SqlError::ShuttingDown))
            }
            EnqueueResult::Full => Err(SqlError::QueueFull),
            EnqueueResult::Closed => Err(SqlError::ShuttingDown),
        }
    }

    async fn write(&self, request: SqlRequest) -> Result<SqlResponse, SqlError> {
        let command = SqlCommand {
            origin: self.node.id(),
            id: self.ids.fetch_add(1, Ordering::Relaxed),
            sql: request.sql,
            params_json: params_json(&request.params)?,
        };
        // One id for this HTTP request. A timed-out forward retries that id
        // so a commit that already happened is not applied twice.
        if self.is_leader().await {
            match self.node.propose(command.clone()).await {
                Ok(response) => return decode_body(&response.body_json),
                Err(ProposeError::NotLeader { .. }) => {}
                Err(ProposeError::Rejected(message)) => return Err(sql_error(message)),
            }
        }
        self.forward(&command).await
    }

    async fn forward(&self, command: &SqlCommand) -> Result<SqlResponse, SqlError> {
        let mut last = "no raft leader".to_string();
        for _ in 0..2 {
            let Some(advertise) = self.node.leader_advertise().await else {
                last = "no raft leader".to_string();
                tokio::time::sleep(Duration::from_millis(30)).await;
                continue;
            };
            let addr = raft_half(&advertise).to_string();
            if addr == self.raft_addr {
                match self.node.propose(command.clone()).await {
                    Ok(response) => return decode_body(&response.body_json),
                    Err(ProposeError::NotLeader { .. }) => last = "not the leader".to_string(),
                    Err(ProposeError::Rejected(message)) => return Err(sql_error(message)),
                }
            } else {
                match client_write(&addr, &self.secret, command).await {
                    Ok(reply) if reply.ok => return decode_body(&reply.body_json),
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

    async fn is_leader(&self) -> bool {
        self.node
            .status()
            .await
            .map(|status| status.role == "leader")
            .unwrap_or(false)
    }
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
