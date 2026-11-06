//! Process startup. Simple mode is the default and does not open a Raft log.
//! Raft mode is chosen once, from the environment, before the HTTP listener starts.

mod exec;
mod wire;

use std::env;
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use osd1_engine::SqliteWorker;
use osd1_http::{router, AppState};
use osd1_raft::{join_cluster, ApplyHook, RaftNode, SqlStore, TcpNode};
use osd1_types::{SqlError, SqlExecutor};

use crate::exec::RaftExec;
use crate::wire::{command_request, encode_result};

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    if raft_configured() {
        let node = start_node(node_config_from_env()?).await?;
        let _ = tokio::signal::ctrl_c().await;
        node.close().await;
        Ok(())
    } else {
        run_simple().await
    }
}

async fn run_simple() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = env::var("OSD1_DB_PATH").unwrap_or_else(|_| "./data/osd1.db".to_string());
    let addr: SocketAddr = env::var("OSD1_HTTP_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8080".to_string())
        .parse()?;
    let api_token = env::var("OSD1_API_TOKEN")
        .ok()
        .filter(|token| !token.is_empty());
    let capacity = env::var("OSD1_QUEUE_CAPACITY")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(64);
    let served_by_colo = env::var("OSD1_SERVED_BY_COLO").unwrap_or_else(|_| "LOC".to_string());

    let path = PathBuf::from(&db_path);
    let worker = SqliteWorker::open(&path, capacity, served_by_colo)?;
    let executor: Arc<dyn SqlExecutor> = Arc::new(worker);
    let mut state = AppState::new(executor, api_token.clone());
    state.max_body_bytes = env_usize("OSD1_MAX_BODY_BYTES", state.max_body_bytes);
    state.max_response_bytes = env_usize("OSD1_MAX_RESPONSE_BYTES", state.max_response_bytes);
    state.request_timeout = match env_u64("OSD1_REQUEST_TIMEOUT_MS", 30_000) {
        0 => None,
        millis => Some(Duration::from_millis(millis)),
    };
    let app = router(state);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    eprintln!("{}", startup_line(&addr, &db_path, api_token.as_deref()));
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

/// One raft process. `bootstrap` and `join` are mutually exclusive.
pub struct NodeConfig {
    pub node_id: u64,
    pub db_path: PathBuf,
    pub http_addr: SocketAddr,
    pub raft_addr: SocketAddr,
    pub secret: String,
    pub bootstrap: bool,
    pub join: Option<String>,
    pub api_token: Option<String>,
    pub colo: String,
    pub capacity: usize,
    pub max_body_bytes: usize,
    pub max_response_bytes: usize,
    pub request_timeout: Option<Duration>,
}

impl NodeConfig {
    pub fn new(node_id: u64, db_path: PathBuf, secret: impl Into<String>) -> Self {
        Self {
            node_id,
            db_path,
            http_addr: "127.0.0.1:0".parse().expect("loopback"),
            raft_addr: "127.0.0.1:0".parse().expect("loopback"),
            secret: secret.into(),
            bootstrap: false,
            join: None,
            api_token: None,
            colo: "LOC".to_string(),
            capacity: 64,
            max_body_bytes: 1024 * 1024,
            max_response_bytes: 1024 * 1024,
            request_timeout: Some(Duration::from_secs(30)),
        }
    }
}

pub struct RunningNode {
    pub node_id: u64,
    pub http_addr: SocketAddr,
    pub raft_addr: SocketAddr,
    http_shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    http_task: Option<tokio::task::JoinHandle<()>>,
    metrics_task: Option<tokio::task::JoinHandle<()>>,
    raft: Option<RaftNode>,
}

impl RunningNode {
    pub async fn close(mut self) {
        self.stop().await;
    }

    async fn stop(&mut self) {
        if let Some(task) = self.metrics_task.take() {
            task.abort();
        }
        if let Some(tx) = self.http_shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.http_task.take() {
            let _ = task.await;
        }
        if let Some(raft) = self.raft.take() {
            let _ = raft.shutdown().await;
        }
    }
}

impl Drop for RunningNode {
    fn drop(&mut self) {
        if let Some(task) = self.metrics_task.take() {
            task.abort();
        }
        if let Some(tx) = self.http_shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.http_task.take() {
            task.abort();
        }
        if let Some(raft) = self.raft.take() {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let _ = raft.shutdown().await;
                });
            }
        }
    }
}

pub async fn start_node(config: NodeConfig) -> Result<RunningNode, std::io::Error> {
    if config.secret.is_empty() {
        return Err(boot("raft secret is empty"));
    }
    if config.bootstrap == config.join.is_some() {
        return Err(boot(
            "raft node needs bootstrap or a join address, not both",
        ));
    }
    let worker = SqliteWorker::open(&config.db_path, config.capacity, config.colo.clone())
        .map_err(|error| boot(error.to_string()))?;
    // Raft replays the committed SQL. Nondeterministic functions would diverge.
    worker.deny_nondeterministic(true);
    let worker = Arc::new(worker);

    let raft_listener = tokio::net::TcpListener::bind(config.raft_addr).await?;
    let raft_addr = raft_listener.local_addr()?;
    let http_listener = tokio::net::TcpListener::bind(config.http_addr).await?;
    let http_addr = http_listener.local_addr()?;
    let advertise = format!("{raft_addr}|{http_addr}");

    let store = SqlStore::new();
    store.set_apply_hook(apply_hook(Arc::clone(&worker)));
    let tcp = TcpNode::start(
        config.node_id,
        raft_listener,
        advertise.clone(),
        config.secret.clone(),
        store,
    )
    .await
    .map_err(|error| boot(error.to_string()))?;
    let raft_node = tcp.node.clone();
    let join_node = raft_node.clone();
    tcp.on_join(Arc::new(move |id, addr| {
        let join_node = join_node.clone();
        Box::pin(async move {
            join_node
                .add_voter(id, addr)
                .await
                .map_err(|error| error.to_string())
        }) as Pin<Box<dyn Future<Output = Result<(), String>> + Send>>
    }));

    if config.bootstrap {
        raft_node
            .bootstrap()
            .await
            .map_err(|error| boot(error.to_string()))?;
    } else if let Some(join) = &config.join {
        join_cluster(join, &config.secret, config.node_id, &advertise)
            .await
            .map_err(boot)?;
        raft_node
            .wait_until(
                |status| status.role == "follower" || status.role == "leader",
                Duration::from_secs(5),
            )
            .await
            .map_err(boot)?;
    }

    let exec = Arc::new(RaftExec::new(
        worker,
        raft_node.clone(),
        Arc::from(config.secret.as_str()),
        raft_addr.to_string(),
    ));
    let executor: Arc<dyn SqlExecutor> = exec;
    let mut state = AppState::new(executor, config.api_token.clone());
    state.max_body_bytes = config.max_body_bytes;
    state.max_response_bytes = config.max_response_bytes;
    state.request_timeout = config.request_timeout;
    state.metrics.enable_raft();
    if let Ok(status) = raft_node.status().await {
        state.metrics.set_raft(
            role_gauge(status.role),
            status.term,
            status.commit_index.unwrap_or(0),
            status.apply_index.unwrap_or(0),
            status.peers,
        );
    }
    let metrics = state.metrics.clone();
    let watch_node = raft_node.clone();
    let app = router(state);
    let (shutdown, stop_rx) = tokio::sync::oneshot::channel();
    let http_task = tokio::spawn(async move {
        let _ = axum::serve(http_listener, app)
            .with_graceful_shutdown(async move {
                let _ = stop_rx.await;
            })
            .await;
    });
    let metrics_task = tokio::spawn(async move {
        loop {
            if let Ok(status) = watch_node.status().await {
                metrics.set_raft(
                    role_gauge(status.role),
                    status.term,
                    status.commit_index.unwrap_or(0),
                    status.apply_index.unwrap_or(0),
                    status.peers,
                );
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    });
    eprintln!(
        "{}",
        startup_line_raft(
            &http_addr,
            &config.db_path.display().to_string(),
            config.node_id,
            &raft_addr,
            config.api_token.as_deref(),
        )
    );
    Ok(RunningNode {
        node_id: config.node_id,
        http_addr,
        raft_addr,
        http_shutdown: Some(shutdown),
        http_task: Some(http_task),
        metrics_task: Some(metrics_task),
        raft: Some(raft_node),
    })
}

/// The raft core awaits this hook, so a slow statement delays heartbeats.
/// The SQL runs on the `osd1-sql` thread. `execute_blocking` waits for a queue
/// slot so a full client queue cannot drop a committed command.
fn apply_hook(worker: Arc<SqliteWorker>) -> ApplyHook {
    Arc::new(move |command| {
        let worker = Arc::clone(&worker);
        Box::pin(async move {
            let request = match command_request(&command.sql, &command.params_json) {
                Ok(request) => request,
                Err(error) => return encode_result(Err(error)),
            };
            let result = tokio::task::spawn_blocking(move || worker.execute_blocking(request))
                .await
                .unwrap_or(Err(SqlError::ShuttingDown));
            encode_result(result)
        })
    })
}

fn role_gauge(role: &str) -> i64 {
    match role {
        "leader" => 1,
        "candidate" => 2,
        "learner" => 3,
        _ => 0,
    }
}

pub(crate) fn sql_error(message: impl Into<String>) -> SqlError {
    SqlError::Sqlite {
        code: 1,
        message: message.into(),
    }
}

fn boot(message: impl Into<String>) -> std::io::Error {
    std::io::Error::other(message.into())
}

fn raft_configured() -> bool {
    env_nonempty("OSD1_NODE_ID").is_some()
        || env_nonempty("OSD1_RAFT_LISTEN").is_some()
        || env_nonempty("OSD1_RAFT_SECRET").is_some()
        || env_nonempty("OSD1_RAFT_JOIN").is_some()
        || env_nonempty("OSD1_RAFT_BOOTSTRAP").is_some()
}

fn node_config_from_env() -> Result<NodeConfig, std::io::Error> {
    let node_id = env_nonempty("OSD1_NODE_ID")
        .ok_or_else(|| boot("OSD1_NODE_ID is required in raft mode"))?
        .parse::<u64>()
        .map_err(|_| boot("OSD1_NODE_ID is not a u64"))?;
    let raft_addr = env_nonempty("OSD1_RAFT_LISTEN")
        .ok_or_else(|| boot("OSD1_RAFT_LISTEN is required in raft mode"))?
        .parse::<SocketAddr>()
        .map_err(|_| boot("OSD1_RAFT_LISTEN is not host:port"))?;
    let secret = env_nonempty("OSD1_RAFT_SECRET")
        .ok_or_else(|| boot("OSD1_RAFT_SECRET is required in raft mode"))?;
    let bootstrap = matches!(
        env::var("OSD1_RAFT_BOOTSTRAP").ok().as_deref(),
        Some("1") | Some("true")
    );
    let join = env_nonempty("OSD1_RAFT_JOIN");
    if bootstrap && join.is_some() {
        return Err(boot(
            "set OSD1_RAFT_BOOTSTRAP=1 or OSD1_RAFT_JOIN, not both",
        ));
    }
    if !bootstrap && join.is_none() {
        return Err(boot("set OSD1_RAFT_BOOTSTRAP=1 or OSD1_RAFT_JOIN"));
    }
    let db_path = env::var("OSD1_DB_PATH").unwrap_or_else(|_| "./data/osd1.db".to_string());
    let http_addr = env::var("OSD1_HTTP_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8080".to_string())
        .parse::<SocketAddr>()
        .map_err(|_| boot("OSD1_HTTP_ADDR is not host:port"))?;
    let api_token = env::var("OSD1_API_TOKEN")
        .ok()
        .filter(|token| !token.is_empty());
    let mut config = NodeConfig::new(node_id, PathBuf::from(db_path), secret);
    config.http_addr = http_addr;
    config.raft_addr = raft_addr;
    config.bootstrap = bootstrap;
    config.join = join;
    config.api_token = api_token;
    config.colo = env::var("OSD1_SERVED_BY_COLO").unwrap_or_else(|_| "LOC".to_string());
    config.capacity = env::var("OSD1_QUEUE_CAPACITY")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(64);
    config.max_body_bytes = env_usize("OSD1_MAX_BODY_BYTES", config.max_body_bytes);
    config.max_response_bytes = env_usize("OSD1_MAX_RESPONSE_BYTES", config.max_response_bytes);
    config.request_timeout = match env_u64("OSD1_REQUEST_TIMEOUT_MS", 30_000) {
        0 => None,
        millis => Some(Duration::from_millis(millis)),
    };
    Ok(config)
}

fn env_nonempty(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.is_empty())
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn startup_line(addr: &SocketAddr, db_path: &str, token: Option<&str>) -> String {
    redact(
        &format!("osd1 listening on {addr} database {db_path}"),
        token,
    )
}

fn startup_line_raft(
    addr: &SocketAddr,
    db_path: &str,
    node_id: u64,
    raft_addr: &SocketAddr,
    token: Option<&str>,
) -> String {
    redact(
        &format!("osd1 listening on {addr} database {db_path} raft node {node_id} at {raft_addr}"),
        token,
    )
}

fn redact(text: &str, secret: Option<&str>) -> String {
    match secret {
        Some(secret) if !secret.is_empty() => text.replace(secret, "[redacted]"),
        _ => text.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn startup_line_redacts_the_api_token() {
        let line = startup_line(
            &"127.0.0.1:8080".parse().unwrap(),
            "/data/topsecret-token/osd1.db",
            Some("topsecret-token"),
        );
        assert!(!line.contains("topsecret-token"));
        assert!(line.contains("[redacted]"));
        assert!(line.contains("osd1 listening"));
    }
}
