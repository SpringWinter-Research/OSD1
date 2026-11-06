//! Optional Raft replication of SQL commands.
//!
//! This crate is not the OSD1 server. A parent process drives an in-process
//! cluster through [`RaftNode`]. Simple mode never constructs one.
//!
//! Storage is openraft 0.9 [`openraft::RaftStorage`] behind
//! [`openraft::storage::Adaptor`]. The `storage-v2` feature is off.
//! `single-term-leader` is on: a term has at most one leader, which is the
//! Raft paper's election-safety rule. Openraft's default vote order does not
//! give that by itself.
//!
//! # Public API
//!
//! - [`RaftNode::start`] registers a node on the shared [`Peers`] transport.
//! - [`RaftNode::bootstrap`] initializes a one-node cluster. Call it once.
//! - [`RaftNode::add_voter`] commits a membership change after the learner catches up.
//! - [`RaftNode::propose`] waits until the entry is committed and applied here.
//!   A follower returns [`ProposeError::NotLeader`].
//! - [`RaftNode::applied`] is the state machine's command list. A repeated
//!   `(origin, id)` pair is stored once.
//! - [`Peers::isolate`] / [`Peers::heal`] drop or restore RPCs.

#![allow(clippy::result_large_err)]

mod network;
mod node;
mod store;
mod tcp;
mod types;

#[cfg(test)]
mod tests;

pub use network::Peers;
pub use node::RaftNode;
pub use node::Status;
pub use store::ApplyHook;
pub use store::SqlStore;
pub use tcp::client_write;
pub use tcp::join_cluster;
pub use tcp::ClientWriteReply;
pub use tcp::TcpNode;
pub use types::NodeId;
pub use types::ProposeError;
pub use types::SqlCommand;
pub use types::SqlResponse;
pub use types::StartError;
pub use types::TypeConfig;
