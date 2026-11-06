use std::collections::BTreeMap;
use std::fmt::Debug;
use std::future::Future;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::pin::Pin;
use std::sync::Arc;

use openraft::storage::RaftSnapshotBuilder;
use openraft::BasicNode;
use openraft::Entry;
use openraft::EntryPayload;
use openraft::LogId;
use openraft::LogState;
use openraft::OptionalSend;
use openraft::RaftLogReader;
use openraft::RaftStorage;
use openraft::Snapshot;
use openraft::SnapshotMeta;
use openraft::StorageError;
use openraft::StorageIOError;
use openraft::StoredMembership;
use openraft::Vote;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::RwLock;

use crate::types::NodeId;
use crate::types::SqlCommand;
use crate::types::SqlResponse;
use crate::types::TypeConfig;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Machine {
    last_applied_log: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
    /// `(origin, id)` to the hook's JSON. A repeated pair does not run again.
    seen: BTreeMap<(u64, u64), String>,
    applied: Vec<SqlCommand>,
}

/// Runs a committed command. The string is stored and returned to the leader's client.
pub type ApplyHook =
    Arc<dyn Fn(SqlCommand) -> Pin<Box<dyn Future<Output = String> + Send>> + Send + Sync>;

struct StoreInner {
    last_purged_log_id: Option<LogId<NodeId>>,
    log: BTreeMap<u64, Entry<TypeConfig>>,
    machine: Machine,
    vote: Option<Vote<NodeId>>,
    snapshot_idx: u64,
    snapshot: Option<(SnapshotMeta<NodeId, BasicNode>, Vec<u8>)>,
}

/// In-memory [`RaftStorage`]. The log and the vote live for the process.
/// A parent that wants disk durability wraps this later. Tests rebuild a
/// node only by keeping this store, which is what the leader-loss test does
/// not need: committed entries are already on the surviving nodes.
#[derive(Clone)]
pub struct SqlStore {
    inner: Arc<RwLock<StoreInner>>,
    apply_hook: Arc<std::sync::RwLock<Option<ApplyHook>>>,
}

impl SqlStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(StoreInner {
                last_purged_log_id: None,
                log: BTreeMap::new(),
                machine: Machine::default(),
                vote: None,
                snapshot_idx: 0,
                snapshot: None,
            })),
            apply_hook: Arc::new(std::sync::RwLock::new(None)),
        }
    }

    /// Called once for each new `(origin, id)`, after the entry is committed.
    /// A repeated pair does not call the hook.
    pub fn set_apply_hook(&self, hook: ApplyHook) {
        *self.apply_hook.write().expect("apply hook") = Some(hook);
    }

    pub async fn new_async() -> Self {
        Self::new()
    }

    pub async fn applied(&self) -> Vec<SqlCommand> {
        self.inner.read().await.machine.applied.clone()
    }
}

impl Default for SqlStore {
    fn default() -> Self {
        Self::new()
    }
}

impl RaftLogReader<TypeConfig> for SqlStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let inner = self.inner.read().await;
        Ok(inner
            .log
            .range(range)
            .map(|(_, entry)| entry.clone())
            .collect())
    }
}

impl RaftSnapshotBuilder<TypeConfig> for SqlStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let mut inner = self.inner.write().await;
        let data = serde_json::to_vec(&inner.machine)
            .map_err(|error| StorageError::from(StorageIOError::read_state_machine(&error)))?;
        let last_applied_log = inner.machine.last_applied_log;
        let last_membership = inner.machine.last_membership.clone();
        inner.snapshot_idx += 1;
        let snapshot_id = match last_applied_log {
            Some(log_id) => format!("{}-{}", log_id.index, inner.snapshot_idx),
            None => format!("empty-{}", inner.snapshot_idx),
        };
        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };
        inner.snapshot = Some((meta.clone(), data.clone()));
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStorage<TypeConfig> for SqlStore {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.inner.write().await.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.read().await.vote)
    }

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let inner = self.inner.read().await;
        let last = inner
            .log
            .iter()
            .next_back()
            .map(|(_, entry)| entry.log_id)
            .or(inner.last_purged_log_id);
        Ok(LogState {
            last_purged_log_id: inner.last_purged_log_id,
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
    {
        let mut inner = self.inner.write().await;
        for entry in entries {
            inner.log.insert(entry.log_id.index, entry);
        }
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.write().await;
        inner.log.split_off(&log_id.index);
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.write().await;
        debug_assert!(inner.last_purged_log_id <= Some(log_id));
        inner.last_purged_log_id = Some(log_id);
        inner.log = inner.log.split_off(&(log_id.index.saturating_add(1)));
        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let inner = self.inner.read().await;
        Ok((
            inner.machine.last_applied_log,
            inner.machine.last_membership.clone(),
        ))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<TypeConfig>],
    ) -> Result<Vec<SqlResponse>, StorageError<NodeId>> {
        let hook = self.apply_hook.read().expect("apply hook").clone();
        enum Plan {
            Done(SqlResponse),
            Run(SqlCommand),
        }
        let mut planned = Vec::with_capacity(entries.len());
        {
            let mut inner = self.inner.write().await;
            for entry in entries {
                inner.machine.last_applied_log = Some(entry.log_id);
                match &entry.payload {
                    EntryPayload::Blank => planned.push(Plan::Done(empty_response(false))),
                    EntryPayload::Normal(command) => {
                        let key = (command.origin, command.id);
                        if let Some(body) = inner.machine.seen.get(&key) {
                            planned.push(Plan::Done(SqlResponse {
                                duplicate: true,
                                body_json: body.clone(),
                            }));
                            continue;
                        }
                        // Empty until the hook returns. A retry in that window is a
                        // duplicate with an empty body. The log is in memory.
                        inner.machine.seen.insert(key, String::new());
                        inner.machine.applied.push(command.clone());
                        planned.push(Plan::Run(command.clone()));
                    }
                    EntryPayload::Membership(membership) => {
                        inner.machine.last_membership =
                            StoredMembership::new(Some(entry.log_id), membership.clone());
                        planned.push(Plan::Done(empty_response(false)));
                    }
                }
            }
        }
        let mut responses = Vec::with_capacity(planned.len());
        for plan in planned {
            match plan {
                Plan::Done(response) => responses.push(response),
                Plan::Run(command) => {
                    let body_json = match &hook {
                        Some(hook) => hook(command.clone()).await,
                        None => String::new(),
                    };
                    let key = (command.origin, command.id);
                    self.inner
                        .write()
                        .await
                        .machine
                        .seen
                        .insert(key, body_json.clone());
                    responses.push(SqlResponse {
                        duplicate: false,
                        body_json,
                    });
                }
            }
        }
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let bytes = snapshot.into_inner();
        let machine: Machine = serde_json::from_slice(&bytes).map_err(|error| {
            StorageError::from(StorageIOError::read_snapshot(
                Some(meta.signature()),
                &error,
            ))
        })?;
        let mut inner = self.inner.write().await;
        inner.machine = machine;
        inner.snapshot = Some((meta.clone(), bytes));
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let inner = self.inner.read().await;
        Ok(inner.snapshot.as_ref().map(|(meta, data)| Snapshot {
            meta: meta.clone(),
            snapshot: Box::new(Cursor::new(data.clone())),
        }))
    }
}

fn empty_response(duplicate: bool) -> SqlResponse {
    SqlResponse {
        duplicate,
        body_json: String::new(),
    }
}
