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
use std::time::{Duration, Instant};

use osd1_engine::SqliteWorker;
use osd1_http::{router, AppState};
use osd1_raft::{
    join_cluster, raft_dir, raft_dir_has_state, remove_member, ApplyHook, BodyHorizonHook,
    InstallHook, RaftNode, SnapshotHook, SqlStore, TcpNode,
};
use osd1_types::{SqlError, SqlExecutor};

use crate::exec::RaftExec;
use crate::wire::{command_request, encode_result};

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    if env_nonempty("OSD1_RAFT_REMOVE").is_some() {
        return remove_from_cluster().await;
    }
    if matches!(
        env::var("OSD1_ROUTER").ok().as_deref(),
        Some("1") | Some("true")
    ) {
        return run_router().await;
    }
    if raft_configured() {
        let mut node = match start_node(node_config_from_env()?).await {
            Ok(node) => node,
            Err(error) if error.to_string() == "removed from the raft cluster" => {
                eprintln!("osd1 node was removed from the raft cluster");
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        };
        node.until_stop().await;
        node.close().await;
        Ok(())
    } else {
        run_simple().await
    }
}

async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = terminate.recv() => {}
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
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
    apply_route_env(&mut state)?;
    apply_litestream_env(&state);
    let app = router(state);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    eprintln!("{}", startup_line(&addr, &db_path, api_token.as_deref()));
    axum::serve(listener, app)
        .with_graceful_shutdown(wait_for_shutdown())
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
    /// Promote this join to a voter. Readers leave this false and stay learners.
    pub voter: bool,
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
            voter: false,
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
    stop: tokio::sync::watch::Sender<bool>,
    removed: std::sync::Arc<std::sync::atomic::AtomicBool>,
    http_task: Option<tokio::task::JoinHandle<()>>,
    metrics_task: Option<tokio::task::JoinHandle<()>>,
    raft: Option<RaftNode>,
}

impl RunningNode {
    /// Waits for SIGINT, SIGTERM, or for this node to leave the membership.
    pub async fn until_stop(&mut self) {
        let mut stop = self.stop.subscribe();
        tokio::select! {
            _ = wait_for_shutdown() => {}
            changed = stop.wait_for(|stop| *stop) => {
                let _ = changed;
            }
        }
        if self.removed.load(std::sync::atomic::Ordering::Relaxed) {
            eprintln!(
                "osd1 node {} was removed from the raft cluster",
                self.node_id
            );
        }
    }

    pub async fn close(mut self) {
        self.stop().await;
    }

    async fn stop(&mut self) {
        let _ = self.stop.send(true);
        if let Some(task) = self.metrics_task.take() {
            task.abort();
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
        let _ = self.stop.send(true);
        if let Some(task) = self.metrics_task.take() {
            task.abort();
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
    if raft_dir(&config.db_path).join("removed").is_file() {
        return Err(boot("removed from the raft cluster"));
    }
    if config.bootstrap && config.join.is_some() {
        return Err(boot(
            "raft node needs bootstrap or a join address, not both",
        ));
    }
    let store =
        SqlStore::open(raft_dir(&config.db_path)).map_err(|error| boot(error.to_string()))?;
    let resume = store.has_state().await;
    if !resume && !config.bootstrap && config.join.is_none() {
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
    // Peers dial the advertised name. A bind on 0.0.0.0 is not that name.
    let raft_public = choose_advertise(
        &raft_addr.to_string(),
        env_nonempty("OSD1_RAFT_ADVERTISE").as_deref(),
        "OSD1_RAFT_ADVERTISE",
    )?;
    let http_public = choose_advertise(
        &http_addr.to_string(),
        env_nonempty("OSD1_HTTP_ADVERTISE").as_deref(),
        "OSD1_HTTP_ADVERTISE",
    )?;
    let advertise = format!("{raft_public}|{http_public}");

    store.set_apply_hook(apply_hook(Arc::clone(&worker)));
    store.set_body_horizon_hook(body_horizon_hook(Arc::clone(&worker)));
    store.set_snapshot_hook(snapshot_hook(Arc::clone(&worker)));
    store.set_install_hook(install_hook(Arc::clone(&worker)));
    let id_store = store.clone();
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
    let voter_join = config.voter;
    tcp.on_join(Arc::new(move |id, addr, voter| {
        let join_node = join_node.clone();
        Box::pin(async move {
            if voter {
                join_node
                    .add_voter(id, addr)
                    .await
                    .map_err(|error| error.to_string())
            } else {
                join_node
                    .add_learner(id, addr)
                    .await
                    .map_err(|error| error.to_string())
            }
        }) as Pin<Box<dyn Future<Output = Result<(), String>> + Send>>
    }));

    if resume {
        // The log and the vote are already on disk. Do not initialize or join again.
    } else if config.bootstrap {
        raft_node
            .bootstrap()
            .await
            .map_err(|error| boot(error.to_string()))?;
    } else if let Some(join) = &config.join {
        join_cluster(join, &config.secret, config.node_id, &advertise, voter_join)
            .await
            .map_err(boot)?;
    }
    // A restarted voter cannot win an election until a majority is bound to
    // the same raft addresses. Leave the process up so the others can vote.
    let ready = raft_node
        .wait_until(
            |status| {
                status.role == "follower" || status.role == "leader" || status.role == "learner"
            },
            Duration::from_secs(5),
        )
        .await;
    if let Err(error) = ready {
        if !resume {
            return Err(boot(error));
        }
    }
    let (was_member, is_member) = raft_node.membership_fate(config.node_id).await;
    if was_member && !is_member {
        let _ = raft_node.shutdown().await;
        return Err(boot("removed from the raft cluster"));
    }

    let raft_max = id_store.max_command_id(config.node_id).await;
    let applied_max = {
        let worker = Arc::clone(&worker);
        let origin = config.node_id;
        tokio::task::spawn_blocking(move || worker.max_applied_id(origin))
            .await
            .map_err(|error| boot(error.to_string()))?
            .map_err(|error| boot(error.to_string()))?
    };
    // `osd1_applied` lives in the database file. A new raft directory after a
    // restore does not, so the next id also has to clear those rows.
    let floor = raft_max.max(applied_max).saturating_add(1);
    let command_ids = crate::exec::CommandIds::load(&raft_dir(&config.db_path), floor)
        .map_err(|error| boot(error.to_string()))?;
    let exec = Arc::new(RaftExec::new(
        worker,
        raft_node.clone(),
        Arc::from(config.secret.as_str()),
        raft_public.clone(),
        command_ids,
        config.request_timeout,
    ));
    let executor: Arc<dyn SqlExecutor> = exec;
    let mut state = AppState::new(Arc::clone(&executor), config.api_token.clone());
    state.max_body_bytes = config.max_body_bytes;
    state.max_response_bytes = config.max_response_bytes;
    // Reads apply `request_timeout` inside RaftExec. Writes wait for raft so a
    // commit is not turned into HTTP 504 and then inserted again on retry.
    state.request_timeout = None;
    apply_route_env(&mut state)?;
    apply_litestream_env(&state);
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
    let draining = Arc::clone(&state.draining);
    let drain_exec = executor;
    let watch_node = raft_node.clone();
    let fate_node = raft_node.clone();
    let app = router(state);
    let (stop, mut http_stop) = tokio::sync::watch::channel(false);
    let removed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let http_task = tokio::spawn(async move {
        let _ = axum::serve(http_listener, app)
            .with_graceful_shutdown(async move {
                let _ = http_stop.wait_for(|stop| *stop).await;
            })
            .await;
    });
    let metrics_stop = stop.clone();
    let removed_flag = std::sync::Arc::clone(&removed);
    let node_id = config.node_id;
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
            let (was_member, is_member) = fate_node.membership_fate(node_id).await;
            if fate_node.was_told_removed() || (was_member && !is_member) {
                // The removed file lands before HTTP closes, so a restart exits
                // instead of rejoining. New queries get 503 while the SQL
                // thread finishes what it already accepted.
                fate_node.mark_removed().await;
                draining.store(true, std::sync::atomic::Ordering::Relaxed);
                let drain_until = Instant::now() + Duration::from_secs(5);
                while drain_exec.sql_inflight() > 0 && Instant::now() < drain_until {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                removed_flag.store(true, std::sync::atomic::Ordering::Relaxed);
                let _ = metrics_stop.send(true);
                let _ = fate_node.shutdown().await;
                break;
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
            &raft_public,
            config.api_token.as_deref(),
        )
    );
    Ok(RunningNode {
        node_id: config.node_id,
        http_addr,
        raft_addr,
        stop,
        removed,
        http_task: Some(http_task),
        metrics_task: Some(metrics_task),
        raft: Some(raft_node),
    })
}

/// Openraft runs this on the state-machine worker, not the raft core.
/// Heartbeats keep moving while SQLite is busy. The SQL runs on the `osd1-sql` thread.
/// `execute_idempotent` waits for a queue slot so a full client queue cannot drop a committed command.
fn apply_hook(worker: Arc<SqliteWorker>) -> ApplyHook {
    Arc::new(move |command| {
        let worker = Arc::clone(&worker);
        Box::pin(async move {
            let request = match command_request(&command.sql, &command.params_json) {
                Ok(request) => request,
                Err(error) => return encode_result(Err(error)),
            };
            let origin = command.origin;
            let id = command.id;
            let log_index = command.log_index;
            let result = tokio::task::spawn_blocking(move || {
                worker.execute_idempotent(origin, id, log_index, request)
            })
            .await
            .unwrap_or(Err(SqlError::ShuttingDown));
            encode_result(result)
        })
    })
}

fn body_horizon_hook(worker: Arc<SqliteWorker>) -> BodyHorizonHook {
    Arc::new(move |index| {
        let worker = Arc::clone(&worker);
        Box::pin(async move {
            let _ = tokio::task::spawn_blocking(move || worker.forget_applied_bodies(index)).await;
        })
    })
}

fn snapshot_hook(worker: Arc<SqliteWorker>) -> SnapshotHook {
    Arc::new(move |dest: PathBuf| {
        let worker = Arc::clone(&worker);
        Box::pin(async move {
            tokio::task::spawn_blocking(move || worker.backup_to(&dest))
                .await
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())
        })
    })
}

fn install_hook(worker: Arc<SqliteWorker>) -> InstallHook {
    Arc::new(move |bytes| {
        let worker = Arc::clone(&worker);
        Box::pin(async move {
            tokio::task::spawn_blocking(move || worker.install_main(bytes))
                .await
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())
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

async fn run_router() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = env::var("OSD1_HTTP_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8080".to_string())
        .parse()?;
    let api_token = env::var("OSD1_API_TOKEN")
        .ok()
        .filter(|token| !token.is_empty());
    let executor: Arc<dyn SqlExecutor> = Arc::new(RouterExec);
    let mut state = AppState::new(executor, api_token.clone());
    state.max_body_bytes = env_usize("OSD1_MAX_BODY_BYTES", state.max_body_bytes);
    state.max_response_bytes = env_usize("OSD1_MAX_RESPONSE_BYTES", state.max_response_bytes);
    state.request_timeout = match env_u64("OSD1_REQUEST_TIMEOUT_MS", 30_000) {
        0 => None,
        millis => Some(Duration::from_millis(millis)),
    };
    apply_route_env(&mut state)?;
    if state.routes.is_empty() {
        return Err(boot("OSD1_ROUTES is required when OSD1_ROUTER=1").into());
    }
    apply_litestream_env(&state);
    let app = router(state);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    eprintln!(
        "{}",
        redact(
            &format!("osd1 router listening on {bound}"),
            api_token.as_deref(),
        )
    );
    axum::serve(listener, app)
        .with_graceful_shutdown(wait_for_shutdown())
        .await?;
    Ok(())
}

async fn remove_from_cluster() -> Result<(), Box<dyn std::error::Error>> {
    let id = env_nonempty("OSD1_RAFT_REMOVE")
        .ok_or_else(|| boot("OSD1_RAFT_REMOVE is required"))?
        .parse::<u64>()
        .map_err(|_| boot("OSD1_RAFT_REMOVE is not a u64"))?;
    let addr = env_nonempty("OSD1_RAFT_JOIN").ok_or_else(|| {
        boot("OSD1_RAFT_JOIN must be the leader raft address when removing a member")
    })?;
    let secret = env_nonempty("OSD1_RAFT_SECRET")
        .ok_or_else(|| boot("OSD1_RAFT_SECRET is required in raft mode"))?;
    remove_member(&addr, &secret, id).await.map_err(boot)?;
    eprintln!("osd1 removed raft member {id}");
    Ok(())
}

fn apply_route_env(state: &mut osd1_http::AppState) -> Result<(), std::io::Error> {
    state.local_database_id = env_nonempty("OSD1_DATABASE_ID");
    if let Some(path) = env_nonempty("OSD1_ROUTES") {
        state.routes = load_routes(std::path::Path::new(&path))?;
    }
    Ok(())
}

fn load_routes(
    path: &std::path::Path,
) -> Result<std::collections::HashMap<String, String>, std::io::Error> {
    let text = std::fs::read_to_string(path)?;
    let mut routes = std::collections::HashMap::new();
    for (number, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.split_whitespace();
        let id = parts.next().unwrap_or("");
        let upstream = parts.next().unwrap_or("");
        if id.is_empty() || upstream.is_empty() || parts.next().is_some() {
            return Err(boot(format!(
                "{}:{} expected \"database_id http://host:port\"",
                path.display(),
                number + 1
            )));
        }
        routes.insert(id.to_string(), upstream.to_string());
    }
    Ok(routes)
}

fn apply_litestream_env(state: &osd1_http::AppState) {
    if let Some(path) = env_nonempty("OSD1_LITESTREAM_HEARTBEAT") {
        state
            .metrics
            .enable_litestream(std::path::PathBuf::from(path));
    }
}

/// Router mode never opens a database. A request that misses the route table
/// is rejected before this type is asked to run SQL.
struct RouterExec;

impl SqlExecutor for RouterExec {
    fn enqueue(&self, _request: osd1_types::SqlRequest) -> osd1_types::EnqueueResult {
        osd1_types::EnqueueResult::Closed
    }

    fn sql_inflight(&self) -> usize {
        0
    }

    fn sql_queue_capacity(&self) -> usize {
        0
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
        || env_nonempty("OSD1_RAFT_VOTER").is_some()
        || env_nonempty("OSD1_RAFT_REMOVE").is_some()
        || env_nonempty("OSD1_RAFT_ADVERTISE").is_some()
        || env_nonempty("OSD1_HTTP_ADVERTISE").is_some()
        || env_nonempty("OSD1_NODE_ID_FROM_HOSTNAME").is_some()
        || env_nonempty("OSD1_RAFT_VOTERS").is_some()
}

fn node_config_from_env() -> Result<NodeConfig, std::io::Error> {
    let from_hostname = flag_on("OSD1_NODE_ID_FROM_HOSTNAME");
    if from_hostname && env_nonempty("OSD1_NODE_ID").is_some() {
        return Err(boot(
            "set OSD1_NODE_ID or OSD1_NODE_ID_FROM_HOSTNAME, not both",
        ));
    }
    let ordinal = if from_hostname {
        let host = env_nonempty("HOSTNAME")
            .ok_or_else(|| boot("HOSTNAME is required when OSD1_NODE_ID_FROM_HOSTNAME is set"))?;
        Some(ordinal_from_hostname(&host)?)
    } else {
        None
    };
    let node_id = if let Some(ordinal) = ordinal {
        let base = env_u64("OSD1_NODE_ID_BASE", 1);
        ordinal
            .checked_add(base)
            .ok_or_else(|| boot("OSD1_NODE_ID_BASE overflows"))?
    } else {
        env_nonempty("OSD1_NODE_ID")
            .ok_or_else(|| boot("OSD1_NODE_ID is required in raft mode"))?
            .parse::<u64>()
            .map_err(|_| boot("OSD1_NODE_ID is not a u64"))?
    };
    let raft_addr = env_nonempty("OSD1_RAFT_LISTEN")
        .ok_or_else(|| boot("OSD1_RAFT_LISTEN is required in raft mode"))?
        .parse::<SocketAddr>()
        .map_err(|_| boot("OSD1_RAFT_LISTEN is not host:port"))?;
    let secret = env_nonempty("OSD1_RAFT_SECRET")
        .ok_or_else(|| boot("OSD1_RAFT_SECRET is required in raft mode"))?;
    let mut bootstrap = matches!(
        env::var("OSD1_RAFT_BOOTSTRAP").ok().as_deref(),
        Some("1") | Some("true")
    );
    let mut join = env_nonempty("OSD1_RAFT_JOIN");
    let mut voter = matches!(
        env::var("OSD1_RAFT_VOTER").ok().as_deref(),
        Some("1") | Some("true")
    );
    if let Some(ordinal) = ordinal {
        let voters = match env_nonempty("OSD1_RAFT_VOTERS") {
            Some(value) => {
                let voters = value
                    .parse::<u64>()
                    .map_err(|_| boot("OSD1_RAFT_VOTERS is not a u64"))?;
                if voters == 0 {
                    return Err(boot("OSD1_RAFT_VOTERS must be 1 or more"));
                }
                Some(voters)
            }
            None => None,
        };
        (bootstrap, join, voter) = membership_for_ordinal(ordinal, voters, bootstrap, join, voter);
    } else if env_nonempty("OSD1_RAFT_VOTERS").is_some() {
        return Err(boot(
            "OSD1_RAFT_VOTERS requires OSD1_NODE_ID_FROM_HOSTNAME=1",
        ));
    }
    if bootstrap && join.is_some() {
        return Err(boot(
            "set OSD1_RAFT_BOOTSTRAP=1 or OSD1_RAFT_JOIN, not both",
        ));
    }
    let db_path = env::var("OSD1_DB_PATH").unwrap_or_else(|_| "./data/osd1.db".to_string());
    let resuming = raft_dir_has_state(&raft_dir(std::path::Path::new(&db_path)));
    if !bootstrap && join.is_none() && !resuming {
        return Err(boot("set OSD1_RAFT_BOOTSTRAP=1 or OSD1_RAFT_JOIN"));
    }
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
    config.voter = voter;
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
    raft_addr: &str,
    token: Option<&str>,
) -> String {
    redact(
        &format!("osd1 listening on {addr} database {db_path} raft node {node_id} at {raft_addr}"),
        token,
    )
}

fn flag_on(name: &str) -> bool {
    matches!(env::var(name).ok().as_deref(), Some("1") | Some("true"))
}

/// Trailing `-<ordinal>` of a StatefulSet pod name. `osd1-0` is ordinal 0.
fn ordinal_from_hostname(host: &str) -> Result<u64, std::io::Error> {
    let Some((_, suffix)) = host.rsplit_once('-') else {
        return Err(boot("HOSTNAME must end in -<ordinal>"));
    };
    suffix
        .parse::<u64>()
        .map_err(|_| boot("HOSTNAME must end in -<ordinal>"))
}

/// Ordinal 0 keeps bootstrap and drops join. Later ordinals drop bootstrap.
/// `voters` makes ordinals below that count voters and the rest learners.
fn membership_for_ordinal(
    ordinal: u64,
    voters: Option<u64>,
    mut bootstrap: bool,
    mut join: Option<String>,
    mut voter: bool,
) -> (bool, Option<String>, bool) {
    if ordinal == 0 {
        join = None;
    } else {
        bootstrap = false;
    }
    if let Some(voters) = voters {
        voter = ordinal < voters;
    }
    (bootstrap, join, voter)
}

/// Address peers dial. `advertised` replaces the bound socket when that socket is not reachable.
fn choose_advertise(
    bound: &str,
    advertised: Option<&str>,
    name: &str,
) -> Result<String, std::io::Error> {
    let Some(advertised) = advertised.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(bound.to_string());
    };
    if advertised.contains(|ch: char| ch.is_whitespace()) || !advertised.contains(':') {
        return Err(boot(format!("{name} is not host:port")));
    }
    Ok(advertised.to_string())
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

    #[test]
    fn hostname_ordinal_reads_the_statefulset_suffix() {
        assert_eq!(ordinal_from_hostname("osd1-0").unwrap(), 0);
        assert_eq!(ordinal_from_hostname("prod-osd1-2").unwrap(), 2);
        assert!(ordinal_from_hostname("osd1").is_err());
        assert!(ordinal_from_hostname("osd1-").is_err());
    }

    #[test]
    fn hostname_mode_bootstraps_only_the_first_pod() {
        let peer = Some("osd1-0.osd1-raft.svc:7000".to_string());
        let (bootstrap, join, voter) = membership_for_ordinal(0, Some(3), true, peer.clone(), true);
        assert!(bootstrap);
        assert!(join.is_none());
        assert!(voter);

        let (bootstrap, join, voter) = membership_for_ordinal(1, Some(3), true, peer.clone(), true);
        assert!(!bootstrap);
        assert_eq!(join.as_deref(), Some("osd1-0.osd1-raft.svc:7000"));
        assert!(voter);

        let (_, _, voter) = membership_for_ordinal(3, Some(3), true, peer, true);
        assert!(!voter);
    }

    #[test]
    fn advertise_override_replaces_the_bind_address() {
        assert_eq!(
            choose_advertise("0.0.0.0:7000", None, "OSD1_RAFT_ADVERTISE").unwrap(),
            "0.0.0.0:7000"
        );
        assert_eq!(
            choose_advertise(
                "0.0.0.0:7000",
                Some("osd1-0.osd1-raft.svc:7000"),
                "OSD1_RAFT_ADVERTISE"
            )
            .unwrap(),
            "osd1-0.osd1-raft.svc:7000"
        );
        assert!(choose_advertise("0.0.0.0:7000", Some("no-port"), "OSD1_RAFT_ADVERTISE").is_err());
    }
}
