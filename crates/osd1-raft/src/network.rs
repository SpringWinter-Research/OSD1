use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use openraft::error::InstallSnapshotError;
use openraft::error::RPCError;
use openraft::error::RaftError;
use openraft::error::RemoteError;
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
use tokio::sync::Mutex;

use crate::types::NodeId;
use crate::types::TypeConfig;

#[derive(Debug, thiserror::Error)]
#[error("raft rpc dropped: {0}")]
struct LinkDown(&'static str);

struct PeerMap {
    nodes: HashMap<NodeId, Raft<TypeConfig>>,
    isolated: HashSet<NodeId>,
}

/// In-process transport shared by every node in one cluster.
///
/// [`Peers::isolate`] makes a node drop every RPC it would send or receive.
/// The other nodes keep talking. [`Peers::heal`] restores the node.
#[derive(Clone)]
pub struct Peers {
    inner: Arc<Mutex<PeerMap>>,
}

impl Peers {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(PeerMap {
                nodes: HashMap::new(),
                isolated: HashSet::new(),
            })),
        }
    }

    pub(crate) async fn insert(&self, id: NodeId, raft: Raft<TypeConfig>) {
        self.inner.lock().await.nodes.insert(id, raft);
    }

    pub(crate) async fn remove(&self, id: NodeId) {
        let mut map = self.inner.lock().await;
        map.nodes.remove(&id);
        map.isolated.remove(&id);
    }

    /// Drop RPCs to and from `id` until [`heal`](Self::heal).
    pub async fn isolate(&self, id: NodeId) {
        self.inner.lock().await.isolated.insert(id);
    }

    /// Restore RPCs for `id`.
    pub async fn heal(&self, id: NodeId) {
        self.inner.lock().await.isolated.remove(&id);
    }

    async fn connect(&self, from: NodeId, to: NodeId) -> Result<Raft<TypeConfig>, Unreachable> {
        let map = self.inner.lock().await;
        if map.isolated.contains(&from) || map.isolated.contains(&to) {
            return Err(Unreachable::new(&LinkDown("isolated")));
        }
        map.nodes
            .get(&to)
            .cloned()
            .ok_or_else(|| Unreachable::new(&LinkDown("not registered")))
    }
}

impl Default for Peers {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub(crate) struct Factory {
    peers: Peers,
    from: NodeId,
}

impl Factory {
    pub(crate) fn new(peers: Peers, from: NodeId) -> Self {
        Self { peers, from }
    }
}

impl RaftNetworkFactory<TypeConfig> for Factory {
    type Network = Conn;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        Conn {
            peers: self.peers.clone(),
            from: self.from,
            to: target,
        }
    }
}

pub(crate) struct Conn {
    peers: Peers,
    from: NodeId,
    to: NodeId,
}

impl RaftNetwork<TypeConfig> for Conn {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let raft = self.peers.connect(self.from, self.to).await?;
        raft.append_entries(rpc)
            .await
            .map_err(|error| RPCError::RemoteError(RemoteError::new(self.to, error)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let raft = self.peers.connect(self.from, self.to).await?;
        raft.vote(rpc)
            .await
            .map_err(|error| RPCError::RemoteError(RemoteError::new(self.to, error)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let raft = self.peers.connect(self.from, self.to).await?;
        raft.install_snapshot(rpc)
            .await
            .map_err(|error| RPCError::RemoteError(RemoteError::new(self.to, error)))
    }

    fn backoff(&self) -> openraft::network::Backoff {
        openraft::network::Backoff::new(std::iter::repeat(Duration::from_millis(20)))
    }
}
