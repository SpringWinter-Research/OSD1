use std::io::Cursor;

use openraft::declare_raft_types;
use openraft::BasicNode;
use serde::Deserialize;
use serde::Serialize;

pub type NodeId = u64;

/// One client SQL command. `(origin, id)` is the idempotency key.
///
/// `origin` is the node that accepted the HTTP request. Each node numbers
/// its own requests from 1, so two nodes can both use id 1 without colliding.
/// A retry of the same pair proposes the same command. The state machine
/// applies that pair once.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlCommand {
    pub origin: u64,
    pub id: u64,
    pub sql: String,
    pub params_json: String,
}

/// What the state machine returned for one applied log entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlResponse {
    /// True when `id` was already applied. The command was not run again.
    pub duplicate: bool,
    /// JSON from the apply hook. Empty when this store has no hook.
    pub body_json: String,
}

declare_raft_types!(
    pub TypeConfig:
        D = SqlCommand,
        R = SqlResponse,
        Node = BasicNode,
        SnapshotData = Cursor<Vec<u8>>,
);

#[derive(Debug, thiserror::Error)]
pub enum StartError {
    #[error("{0}")]
    Raft(String),
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ProposeError {
    #[error("not the leader; leader is {leader:?}")]
    NotLeader { leader: Option<NodeId> },
    #[error("{0}")]
    Rejected(String),
}
