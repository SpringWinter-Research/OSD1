use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use openraft::error::ClientWriteError;
use openraft::error::RaftError;
use openraft::storage::Adaptor;
use openraft::BasicNode;
use openraft::ChangeMembers;
use openraft::Config;
use openraft::Raft;
use openraft::ServerState;

use crate::network::Factory;
use crate::network::Peers;
use crate::store::SqlStore;
use crate::types::NodeId;
use crate::types::ProposeError;
use crate::types::SqlCommand;
use crate::types::SqlResponse;
use crate::types::StartError;
use crate::types::TypeConfig;

/// One Raft process. Clone is cheap. Drop does not shut the tasks down;
/// call [`RaftNode::shutdown`] so the cluster stops sending to this id.
#[derive(Clone)]
pub struct RaftNode {
    id: NodeId,
    advertise: String,
    raft: Raft<TypeConfig>,
    store: SqlStore,
    peers: Peers,
    tcp_stop: Option<tokio::sync::watch::Sender<bool>>,
}

/// A point-in-time view. `commit_index` is the quorum-committed log index.
/// `apply_index` is what this node's state machine has consumed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Status {
    pub id: NodeId,
    pub role: &'static str,
    pub term: u64,
    pub leader: Option<NodeId>,
    pub last_log_index: Option<u64>,
    pub commit_index: Option<u64>,
    pub apply_index: Option<u64>,
    /// Nodes in the current membership, including this one.
    pub peers: u64,
}

impl RaftNode {
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// The transport this node was started on. Tests use it to partition a node.
    pub fn transport(&self) -> Peers {
        self.peers.clone()
    }

    /// Register `id` on `peers`. Does not form a cluster. Call [`bootstrap`]
    /// on exactly one node, then [`add_voter`] for the rest.
    pub async fn start(id: NodeId, peers: Peers, store: SqlStore) -> Result<Self, StartError> {
        let factory = Factory::new(peers.clone(), id);
        let node = Self::open(id, factory, store, peers.clone()).await?;
        peers.insert(id, node.raft.clone()).await;
        Ok(node)
    }

    pub(crate) async fn open<N>(
        id: NodeId,
        network: N,
        store: SqlStore,
        peers: Peers,
    ) -> Result<Self, StartError>
    where
        N: openraft::RaftNetworkFactory<TypeConfig>,
    {
        let config = raft_config()?;
        let (log_store, state_machine) = Adaptor::new(store.clone());
        let raft = Raft::new(id, config, network, log_store, state_machine)
            .await
            .map_err(|error| StartError::Raft(error.to_string()))?;
        Ok(Self {
            id,
            advertise: id.to_string(),
            raft,
            store,
            peers,
            tcp_stop: None,
        })
    }

    /// Make this node a single-voter cluster and wait until it is leader.
    /// The other ids in `peers` are not members yet.
    pub async fn bootstrap(&self) -> Result<(), StartError> {
        let mut members = BTreeMap::new();
        members.insert(self.id, BasicNode::new(self.advertise.clone()));
        self.raft
            .initialize(members)
            .await
            .map_err(|error| StartError::Raft(error.to_string()))?;
        self.wait_until(|status| status.role == "leader", Duration::from_secs(3))
            .await
            .map_err(|error| StartError::Raft(error))?;
        Ok(())
    }

    /// Copy the log to `id`, then commit a membership entry that makes it a voter.
    /// `addr` is the endpoint stored in the membership. The in-process transport
    /// ignores it. The TCP transport dials the part before `|`.
    pub async fn add_voter(&self, id: NodeId, addr: impl Into<String>) -> Result<(), ProposeError> {
        self.raft
            .add_learner(id, BasicNode::new(addr.into()), true)
            .await
            .map_err(map_write)?;
        let mut ids = BTreeSet::new();
        ids.insert(id);
        self.raft
            .change_membership(ChangeMembers::AddVoterIds(ids), false)
            .await
            .map_err(map_write)?;
        Ok(())
    }

    /// Propose a command. Returns after this node has applied it.
    /// A follower does not accept the write.
    pub async fn propose(&self, command: SqlCommand) -> Result<SqlResponse, ProposeError> {
        let response = self.raft.client_write(command).await.map_err(map_write)?;
        Ok(response.data)
    }

    pub async fn applied(&self) -> Vec<SqlCommand> {
        self.store.applied().await
    }

    pub async fn status(&self) -> Result<Status, StartError> {
        let metrics = self.raft.metrics().borrow().clone();
        let committed = self
            .raft
            .with_raft_state(|state| state.committed)
            .await
            .map_err(|error| StartError::Raft(error.to_string()))?;
        let membership = metrics.membership_config.membership().clone();
        let peers = membership.nodes().count() as u64;
        Ok(Status {
            id: self.id,
            role: role_name(metrics.state),
            term: metrics.current_term,
            leader: metrics.current_leader,
            last_log_index: metrics.last_log_index,
            commit_index: committed.map(|log_id| log_id.index),
            apply_index: metrics.last_applied.map(|log_id| log_id.index),
            peers,
        })
    }

    pub async fn wait_until<F>(&self, mut ready: F, timeout: Duration) -> Result<(), String>
    where
        F: FnMut(&Status) -> bool,
    {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let status = self.status().await.map_err(|error| error.to_string())?;
            if ready(&status) {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(format!("timed out waiting, last status {status:?}"));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Membership address of the current leader, if this node knows one.
    /// The TCP transport dials the text before `|`.
    pub async fn leader_advertise(&self) -> Option<String> {
        let metrics = self.raft.metrics().borrow().clone();
        let leader = metrics.current_leader?;
        let membership = metrics.membership_config.membership().clone();
        let addr = membership
            .nodes()
            .find(|(id, _)| **id == leader)
            .map(|(_, node)| node.addr.clone());
        addr
    }

    pub async fn shutdown(&self) -> Result<(), StartError> {
        if let Some(stop) = &self.tcp_stop {
            let _ = stop.send(true);
        }
        self.peers.remove(self.id).await;
        self.raft
            .shutdown()
            .await
            .map_err(|error| StartError::Raft(error.to_string()))
    }

    pub(crate) fn finish_tcp(
        mut self,
        advertise: String,
        stop: tokio::sync::watch::Sender<bool>,
    ) -> Self {
        self.advertise = advertise;
        self.tcp_stop = Some(stop);
        self
    }

    pub(crate) fn raft(&self) -> Raft<TypeConfig> {
        self.raft.clone()
    }
}

fn raft_config() -> Result<Arc<Config>, StartError> {
    let config = Config {
        cluster_name: "osd1".into(),
        heartbeat_interval: 50,
        election_timeout_min: 150,
        election_timeout_max: 300,
        snapshot_policy: openraft::SnapshotPolicy::Never,
        ..Config::default()
    };
    config
        .validate()
        .map(Arc::new)
        .map_err(|error| StartError::Raft(error.to_string()))
}

fn role_name(state: ServerState) -> &'static str {
    match state {
        ServerState::Learner => "learner",
        ServerState::Follower => "follower",
        ServerState::Candidate => "candidate",
        ServerState::Leader => "leader",
        ServerState::Shutdown => "shutdown",
    }
}

fn map_write(error: RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>) -> ProposeError {
    match error {
        RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) => {
            ProposeError::NotLeader {
                leader: forward.leader_id,
            }
        }
        RaftError::APIError(other) => ProposeError::Rejected(other.to_string()),
        RaftError::Fatal(fatal) => ProposeError::Rejected(fatal.to_string()),
    }
}
