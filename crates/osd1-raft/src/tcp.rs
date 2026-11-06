//! Length-prefixed Raft RPCs on TCP.
//!
//! A membership address is `raft_host:port` or `raft_host:port|http_host:port`.
//! This transport dials the raft half. The HTTP half is for the OSD1 process.
//! Every frame carries the cluster secret. A mismatch is dropped.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use openraft::error::InstallSnapshotError;
use openraft::error::RPCError;
use openraft::error::RaftError;
use openraft::error::Unreachable;
use openraft::network::RPCOption;
use openraft::network::RaftNetwork;
use openraft::network::RaftNetworkFactory;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::InstallSnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::BasicNode;
use openraft::Raft;
use serde::Deserialize;
use serde::Serialize;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::watch;

use crate::network::Peers;
use crate::node::RaftNode;
use crate::store::SqlStore;
use crate::types::NodeId;
use crate::types::SqlCommand;
use crate::types::StartError;
use crate::types::TypeConfig;

const MAX_FRAME: usize = 16 * 1024 * 1024;
const RPC_TIMEOUT: Duration = Duration::from_millis(800);
const JOIN_TIMEOUT: Duration = Duration::from_secs(10);
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);

const KIND_VOTE: u8 = 1;
const KIND_APPEND: u8 = 2;
const KIND_SNAPSHOT: u8 = 3;
const KIND_JOIN: u8 = 4;
const KIND_WRITE: u8 = 5;

pub type JoinHook = Arc<
    dyn Fn(NodeId, String) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>>
        + Send
        + Sync,
>;

/// A node that accepts Raft RPCs on `listener`.
pub struct TcpNode {
    pub node: RaftNode,
    join_hook: Arc<std::sync::Mutex<Option<JoinHook>>>,
}

impl TcpNode {
    pub async fn start(
        id: NodeId,
        listener: TcpListener,
        advertise: impl Into<String>,
        secret: impl Into<String>,
        store: SqlStore,
    ) -> Result<Self, StartError> {
        let secret = secret.into();
        if secret.is_empty() {
            return Err(StartError::Raft("raft secret is empty".into()));
        }
        let secret: Arc<str> = Arc::from(secret);
        let join_hook = Arc::new(std::sync::Mutex::new(None));
        let (stop_tx, stop_rx) = watch::channel(false);
        let node = RaftNode::open(
            id,
            TcpFactory {
                secret: Arc::clone(&secret),
            },
            store,
            Peers::new(),
        )
        .await?
        .finish_tcp(advertise.into(), stop_tx);
        let raft = node.raft();
        let hook = Arc::clone(&join_hook);
        tokio::spawn(accept_loop(listener, raft, secret, hook, stop_rx));
        Ok(Self { node, join_hook })
    }

    /// Called when a peer asks to become a voter. The hook runs on this process.
    pub fn on_join(&self, hook: JoinHook) {
        *self.join_hook.lock().expect("join hook") = Some(hook);
    }
}

#[derive(Clone)]
struct TcpFactory {
    secret: Arc<str>,
}

impl RaftNetworkFactory<TypeConfig> for TcpFactory {
    type Network = TcpConn;

    async fn new_client(&mut self, _target: NodeId, node: &BasicNode) -> Self::Network {
        TcpConn {
            addr: raft_half(&node.addr).to_string(),
            secret: Arc::clone(&self.secret),
        }
    }
}

struct TcpConn {
    addr: String,
    secret: Arc<str>,
}

impl RaftNetwork<TypeConfig> for TcpConn {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let bytes = call(&self.addr, &self.secret, KIND_APPEND, &rpc).await?;
        decode(&bytes)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let bytes = call(&self.addr, &self.secret, KIND_VOTE, &rpc).await?;
        decode(&bytes)
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let bytes = call(&self.addr, &self.secret, KIND_SNAPSHOT, &rpc).await?;
        decode(&bytes)
    }

    fn backoff(&self) -> openraft::network::Backoff {
        openraft::network::Backoff::new(std::iter::repeat(Duration::from_millis(20)))
    }
}

#[derive(Serialize, Deserialize)]
struct JoinBody {
    id: NodeId,
    advertise: String,
}

#[derive(Serialize, Deserialize)]
struct JoinReply {
    ok: bool,
    error: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClientWriteReply {
    pub ok: bool,
    pub leader: Option<NodeId>,
    pub body_json: String,
    pub error: String,
}

/// Ask `addr` to add this node. `addr` is `host:port` of a node that can commit.
pub async fn join_cluster(
    addr: &str,
    secret: &str,
    id: NodeId,
    advertise: &str,
) -> Result<(), String> {
    let reply: JoinReply = match call_for(
        addr,
        secret,
        KIND_JOIN,
        &JoinBody {
            id,
            advertise: advertise.to_string(),
        },
        JOIN_TIMEOUT,
    )
    .await
    {
        Ok(bytes) => serde_json::from_slice(&bytes).map_err(|error| error.to_string())?,
        Err(error) => return Err(error.to_string()),
    };
    if reply.ok {
        Ok(())
    } else {
        Err(reply.error)
    }
}

/// Send a command to the leader over the raft port. A follower returns `ok: false`
/// and `leader: Some(...)`.
pub async fn client_write(
    addr: &str,
    secret: &str,
    command: &SqlCommand,
) -> Result<ClientWriteReply, String> {
    let bytes = call_for(addr, secret, KIND_WRITE, command, WRITE_TIMEOUT)
        .await
        .map_err(|error| error.to_string())?;
    serde_json::from_slice(&bytes).map_err(|error| error.to_string())
}

async fn accept_loop(
    listener: TcpListener,
    raft: Raft<TypeConfig>,
    secret: Arc<str>,
    join_hook: Arc<std::sync::Mutex<Option<JoinHook>>>,
    mut stop: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            _ = stop.changed() => {
                if *stop.borrow() {
                    break;
                }
            }
            accepted = listener.accept() => {
                let Ok((socket, _)) = accepted else { break };
                let raft = raft.clone();
                let secret = Arc::clone(&secret);
                let join_hook = Arc::clone(&join_hook);
                tokio::spawn(async move {
                    let _ = handle_conn(socket, raft, secret, join_hook).await;
                });
            }
        }
    }
}

async fn handle_conn(
    mut socket: TcpStream,
    raft: Raft<TypeConfig>,
    secret: Arc<str>,
    join_hook: Arc<std::sync::Mutex<Option<JoinHook>>>,
) -> std::io::Result<()> {
    let (kind, body) = read_payload(&mut socket).await?;
    let Some(json) = split_secret(&body, &secret) else {
        return Ok(());
    };
    let response = dispatch(kind, json, raft, join_hook).await;
    write_payload(&mut socket, 0, b"", &response).await
}

async fn dispatch(
    kind: u8,
    json: &[u8],
    raft: Raft<TypeConfig>,
    join_hook: Arc<std::sync::Mutex<Option<JoinHook>>>,
) -> Vec<u8> {
    match kind {
        KIND_VOTE => {
            rpc_result(
                serde_json::from_slice::<VoteRequest<NodeId>>(json),
                |rpc| async move { raft.vote(rpc).await },
            )
            .await
        }
        KIND_APPEND => {
            rpc_result(
                serde_json::from_slice::<AppendEntriesRequest<TypeConfig>>(json),
                |rpc| async move { raft.append_entries(rpc).await },
            )
            .await
        }
        KIND_SNAPSHOT => {
            rpc_result(
                serde_json::from_slice::<InstallSnapshotRequest<TypeConfig>>(json),
                |rpc| async move { raft.install_snapshot(rpc).await },
            )
            .await
        }
        KIND_JOIN => join_dispatch(json, raft, join_hook).await,
        KIND_WRITE => write_dispatch(json, raft).await,
        _ => serde_json::to_vec(&JoinReply {
            ok: false,
            error: "unknown raft frame".into(),
        })
        .unwrap_or_default(),
    }
}

async fn rpc_result<T, R, E, F, Fut>(parsed: Result<T, serde_json::Error>, call: F) -> Vec<u8>
where
    F: FnOnce(T) -> Fut,
    Fut: Future<Output = Result<R, E>>,
    R: Serialize,
    E: std::fmt::Display,
{
    let rpc = match parsed {
        Ok(rpc) => rpc,
        Err(error) => {
            return serde_json::to_vec(&error.to_string()).unwrap_or_default();
        }
    };
    match call(rpc).await {
        Ok(response) => serde_json::to_vec(&response).unwrap_or_default(),
        Err(error) => serde_json::to_vec(&error.to_string()).unwrap_or_default(),
    }
}

async fn join_dispatch(
    json: &[u8],
    raft: Raft<TypeConfig>,
    join_hook: Arc<std::sync::Mutex<Option<JoinHook>>>,
) -> Vec<u8> {
    let request: JoinBody = match serde_json::from_slice(json) {
        Ok(request) => request,
        Err(error) => {
            return json_reply(JoinReply {
                ok: false,
                error: error.to_string(),
            })
        }
    };
    let metrics = raft.metrics().borrow().clone();
    if metrics.state != openraft::ServerState::Leader {
        return json_reply(JoinReply {
            ok: false,
            error: format!("not the leader; leader is {:?}", metrics.current_leader),
        });
    }
    let hook = join_hook.lock().expect("join hook").clone();
    let Some(hook) = hook else {
        return json_reply(JoinReply {
            ok: false,
            error: "join is not open".into(),
        });
    };
    match hook(request.id, request.advertise).await {
        Ok(()) => json_reply(JoinReply {
            ok: true,
            error: String::new(),
        }),
        Err(error) => json_reply(JoinReply { ok: false, error }),
    }
}

async fn write_dispatch(json: &[u8], raft: Raft<TypeConfig>) -> Vec<u8> {
    let command: SqlCommand = match serde_json::from_slice(json) {
        Ok(command) => command,
        Err(error) => {
            return json_reply(ClientWriteReply {
                ok: false,
                leader: None,
                body_json: String::new(),
                error: error.to_string(),
            })
        }
    };
    match raft.client_write(command).await {
        Ok(response) => json_reply(ClientWriteReply {
            ok: true,
            leader: Some(raft.metrics().borrow().id),
            body_json: response.data.body_json,
            error: String::new(),
        }),
        Err(error) => {
            let leader = error
                .forward_to_leader()
                .and_then(|forward| forward.leader_id);
            json_reply(ClientWriteReply {
                ok: false,
                leader,
                body_json: String::new(),
                error: error.to_string(),
            })
        }
    }
}

fn json_reply(body: impl Serialize) -> Vec<u8> {
    serde_json::to_vec(&body).unwrap_or_default()
}

async fn call<T: Serialize>(
    addr: &str,
    secret: &str,
    kind: u8,
    body: &T,
) -> Result<Vec<u8>, Unreachable> {
    call_for(addr, secret, kind, body, RPC_TIMEOUT).await
}

async fn call_for<T: Serialize>(
    addr: &str,
    secret: &str,
    kind: u8,
    body: &T,
    timeout: Duration,
) -> Result<Vec<u8>, Unreachable> {
    let json = serde_json::to_vec(body).map_err(|error| down(&error))?;
    let result = tokio::time::timeout(timeout, async {
        let mut socket = TcpStream::connect(addr)
            .await
            .map_err(|error| down(&error))?;
        write_payload(&mut socket, kind, secret.as_bytes(), &json)
            .await
            .map_err(|error| down(&error))?;
        let (_kind, payload) = read_payload(&mut socket)
            .await
            .map_err(|error| down(&error))?;
        let json = json_after_secret(&payload).map_err(|error| down(&error))?;
        Ok(json)
    })
    .await;
    match result {
        Ok(Ok(bytes)) => Ok(bytes),
        Ok(Err(error)) => Err(error),
        Err(_) => Err(down(&std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "raft rpc timed out",
        ))),
    }
}

fn decode<T: for<'a> Deserialize<'a>, E: std::error::Error>(
    bytes: &[u8],
) -> Result<T, RPCError<NodeId, BasicNode, E>> {
    serde_json::from_slice(bytes).map_err(|error| RPCError::Unreachable(down(&error)))
}

fn down(error: &(impl std::error::Error + 'static)) -> Unreachable {
    Unreachable::new(error)
}

fn raft_half(addr: &str) -> &str {
    addr.split('|').next().unwrap_or(addr)
}

fn json_after_secret(body: &[u8]) -> std::io::Result<Vec<u8>> {
    if body.len() < 2 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "raft reply header",
        ));
    }
    let len = u16::from_be_bytes([body[0], body[1]]) as usize;
    let rest = body
        .get(2..)
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "raft reply header"))?;
    let json = rest
        .get(len..)
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "raft reply secret"))?;
    Ok(json.to_vec())
}

fn split_secret<'a>(body: &'a [u8], secret: &str) -> Option<&'a [u8]> {
    if body.len() < 2 {
        return None;
    }
    let len = u16::from_be_bytes([body[0], body[1]]) as usize;
    let rest = body.get(2..)?;
    let (got, json) = rest.split_at_checked(len)?;
    if secrets_match(got, secret.as_bytes()) {
        Some(json)
    } else {
        None
    }
}

fn secrets_match(got: &[u8], expect: &[u8]) -> bool {
    if got.len() != expect.len() {
        return false;
    }
    let mut diff = 0u8;
    for (left, right) in got.iter().zip(expect.iter()) {
        diff |= left ^ right;
    }
    diff == 0
}

async fn read_payload(socket: &mut TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
    let mut len_buf = [0u8; 4];
    socket.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len == 0 || len > MAX_FRAME {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "raft frame length",
        ));
    }
    let mut buf = vec![0u8; len];
    socket.read_exact(&mut buf).await?;
    let kind = buf[0];
    Ok((kind, buf[1..].to_vec()))
}

async fn write_payload(
    socket: &mut TcpStream,
    kind: u8,
    secret: &[u8],
    json: &[u8],
) -> std::io::Result<()> {
    if secret.len() > u16::MAX as usize {
        return Err(std::io::Error::other("raft secret is too long"));
    }
    let mut payload = Vec::with_capacity(1 + 2 + secret.len() + json.len());
    payload.push(kind);
    payload.extend_from_slice(&(secret.len() as u16).to_be_bytes());
    payload.extend_from_slice(secret);
    payload.extend_from_slice(json);
    if payload.len() > MAX_FRAME {
        return Err(std::io::Error::other("raft frame is too large"));
    }
    socket
        .write_all(&(payload.len() as u32).to_be_bytes())
        .await?;
    socket.write_all(&payload).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::net::TcpListener;

    use super::*;
    use crate::SqlCommand;

    async fn listen() -> (TcpListener, String) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        (listener, format!("127.0.0.1:{port}"))
    }

    #[tokio::test]
    async fn three_tcp_nodes_commit_and_a_bad_secret_is_refused() {
        let secret = "cluster-secret";
        let (l1, a1) = listen().await;
        let (l2, a2) = listen().await;
        let (l3, a3) = listen().await;
        let n1 = TcpNode::start(1, l1, a1.clone(), secret, SqlStore::new())
            .await
            .unwrap();
        let n2 = TcpNode::start(2, l2, a2.clone(), secret, SqlStore::new())
            .await
            .unwrap();
        let n3 = TcpNode::start(3, l3, a3.clone(), secret, SqlStore::new())
            .await
            .unwrap();
        n1.node.bootstrap().await.unwrap();
        n1.node.add_voter(2, a2.clone()).await.unwrap();
        n1.node.add_voter(3, a3.clone()).await.unwrap();
        n1.node
            .propose(SqlCommand {
                origin: 1,
                id: 1,
                sql: "create table t(n integer)".into(),
                params_json: "[]".into(),
            })
            .await
            .unwrap();
        for node in [&n1, &n2, &n3] {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            loop {
                if node
                    .node
                    .applied()
                    .await
                    .iter()
                    .any(|command| command.id == 1)
                {
                    break;
                }
                assert!(tokio::time::Instant::now() < deadline);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        let refused = join_cluster(&a1, "nope", 9, "127.0.0.1:1").await;
        assert!(refused.is_err(), "{refused:?}");
        for node in [&n1, &n2, &n3] {
            node.node.shutdown().await.unwrap();
        }
    }
}
