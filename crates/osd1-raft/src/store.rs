use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::io::{Cursor, Seek, SeekFrom, Write};
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
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
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::RwLock;

use crate::types::NodeId;
use crate::types::SqlCommand;
use crate::types::SqlResponse;
use crate::types::TypeConfig;

const SNAP_MAGIC: &[u8] = b"OSD1SNAP1";
const LOG_MAGIC: &[u8] = b"OSD1LOG2";

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Machine {
    last_applied_log: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
    /// In-memory only. A durable store keeps the result in `osd1_applied`.
    #[serde(skip)]
    seen: BTreeMap<String, String>,
    /// In-memory only. Reloaded from the log. The SQL file is the durable copy.
    #[serde(skip)]
    applied: Vec<SqlCommand>,
    /// Node ids that have appeared in an applied membership. The log can drop
    /// those entries after a snapshot, and a removed member still has to notice.
    #[serde(default)]
    member_ids: BTreeSet<u64>,
}

impl Machine {
    fn note(&mut self, membership: &openraft::Membership<NodeId, BasicNode>) {
        for (id, _) in membership.nodes() {
            self.member_ids.insert(*id);
        }
    }
}

#[derive(Serialize, Deserialize)]
struct DiskState {
    vote: Option<Vote<NodeId>>,
    last_purged_log_id: Option<LogId<NodeId>>,
    snapshot_idx: u64,
    machine: Machine,
    snapshot_meta: Option<SnapshotMeta<NodeId, BasicNode>>,
}

/// Runs a committed command. The string is stored and returned to the leader's client.
///
/// Openraft runs this on the state-machine worker, which is not the raft core.
/// A slow statement does not delay heartbeats.
pub type ApplyHook =
    Arc<dyn Fn(SqlCommand) -> Pin<Box<dyn Future<Output = String> + Send>> + Send + Sync>;

/// Clear stored SQL bodies at or below a purged log index. The `(origin, id)` row stays.
pub type BodyHorizonHook =
    Arc<dyn Fn(u64) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// Copy SQLite pages into `dest`. The file is a separate database, not a second connection
/// to the live one. Empty when this store has no database.
pub type SnapshotHook =
    Arc<dyn Fn(PathBuf) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> + Send + Sync>;

/// Replace the local SQLite file with the bytes from a snapshot.
pub type InstallHook =
    Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> + Send + Sync>;

struct StoreInner {
    dir: Option<PathBuf>,
    last_purged_log_id: Option<LogId<NodeId>>,
    log: BTreeMap<u64, Entry<TypeConfig>>,
    /// Bytes of `log.dat` that match `log`. Appends write at this offset.
    log_bytes: u64,
    /// Highest index included in `log_bytes`. `None` when the file has no entries.
    log_durable_index: Option<u64>,
    /// `log.dat` uses the checksummed `OSD1LOG2` header. A legacy file is rewritten
    /// on the next append.
    log_v2: bool,
    machine: Machine,
    vote: Option<Vote<NodeId>>,
    snapshot_idx: u64,
    /// `snapshot_idx` last written to `snapshot.bin`. Avoids rewriting the SQLite copy on every log append.
    disk_snapshot_idx: u64,
    snapshot_meta: Option<SnapshotMeta<NodeId, BasicNode>>,
    /// Used when `dir` is `None`. A durable snapshot stays in `snapshot.bin`.
    memory_snapshot: Option<Vec<u8>>,
}

#[derive(Serialize, Deserialize)]
struct MetaFile {
    last_purged_log_id: Option<LogId<NodeId>>,
    snapshot_idx: u64,
    snapshot_meta: Option<SnapshotMeta<NodeId, BasicNode>>,
}

/// Raft log, vote, and membership.
///
/// [`SqlStore::new`] keeps them in memory for the openraft store suite.
/// [`SqlStore::open`] writes them under `dir` and reloads them on the next open.
/// The directory and the SQLite file are one pair.
#[derive(Clone)]
pub struct SqlStore {
    inner: Arc<RwLock<StoreInner>>,
    /// Serializes `meta.json`. The writer re-reads memory so a purge and a
    /// snapshot cannot persist each other's stale fields.
    meta_lock: Arc<tokio::sync::Mutex<()>>,
    apply_hook: Arc<std::sync::RwLock<Option<ApplyHook>>>,
    body_horizon_hook: Arc<std::sync::RwLock<Option<BodyHorizonHook>>>,
    snapshot_hook: Arc<std::sync::RwLock<Option<SnapshotHook>>>,
    install_hook: Arc<std::sync::RwLock<Option<InstallHook>>>,
}

impl SqlStore {
    pub fn new() -> Self {
        Self::from_inner(StoreInner::empty(None))
    }

    /// Load `dir`, or create it. The directory mode is `0700` and the files are `0600`.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        let dir = dir.as_ref().to_path_buf();
        create_private_dir(&dir)?;
        let inner = StoreInner::load(&dir)?;
        Ok(Self::from_inner(inner))
    }

    fn from_inner(inner: StoreInner) -> Self {
        Self {
            inner: Arc::new(RwLock::new(inner)),
            meta_lock: Arc::new(tokio::sync::Mutex::new(())),
            apply_hook: Arc::new(std::sync::RwLock::new(None)),
            body_horizon_hook: Arc::new(std::sync::RwLock::new(None)),
            snapshot_hook: Arc::new(std::sync::RwLock::new(None)),
            install_hook: Arc::new(std::sync::RwLock::new(None)),
        }
    }

    /// Called once for each new `(origin, id)`, after the entry is committed.
    /// A repeated pair does not call the hook.
    pub fn set_apply_hook(&self, hook: ApplyHook) {
        *self.apply_hook.write().expect("apply hook") = Some(hook);
    }

    /// Called after a log purge. The index is the last purged log index.
    pub fn set_body_horizon_hook(&self, hook: BodyHorizonHook) {
        *self.body_horizon_hook.write().expect("body horizon") = Some(hook);
    }

    pub fn set_snapshot_hook(&self, hook: SnapshotHook) {
        *self.snapshot_hook.write().expect("snapshot hook") = Some(hook);
    }

    pub fn set_install_hook(&self, hook: InstallHook) {
        *self.install_hook.write().expect("install hook") = Some(hook);
    }

    pub async fn new_async() -> Self {
        Self::new()
    }

    pub async fn applied(&self) -> Vec<SqlCommand> {
        self.inner.read().await.machine.applied.clone()
    }

    /// Highest command id this store has for `origin`, or 0.
    ///
    /// The log, the applied list, and the idempotency map all count. A restarted
    /// process uses this when its `next-command-id` file is missing or behind.
    pub async fn max_command_id(&self, origin: u64) -> u64 {
        let inner = self.inner.read().await;
        let mut max_id = 0u64;
        for command in &inner.machine.applied {
            if command.origin == origin {
                max_id = max_id.max(command.id);
            }
        }
        for entry in inner.log.values() {
            if let EntryPayload::Normal(command) = &entry.payload {
                if command.origin == origin {
                    max_id = max_id.max(command.id);
                }
            }
        }
        let prefix = format!("{origin}:");
        for key in inner.machine.seen.keys() {
            if let Some(rest) = key.strip_prefix(&prefix) {
                if let Ok(id) = rest.parse::<u64>() {
                    max_id = max_id.max(id);
                }
            }
        }
        max_id
    }

    /// True after this directory has voted or stored a log entry.
    /// A restart uses this to skip `initialize` and `join`.
    pub async fn has_state(&self) -> bool {
        let inner = self.inner.read().await;
        inner.vote.is_some() || !inner.log.is_empty() || inner.machine.last_applied_log.is_some()
    }

    /// Record that this process was removed. A restart sees the file and exits.
    pub async fn mark_removed(&self) {
        let dir = {
            let inner = self.inner.read().await;
            inner.dir.clone()
        };
        let Some(dir) = dir else {
            return;
        };
        let _ = blocking_disk(move || atomic_write(&dir, "removed", b"1\n")).await;
    }

    /// `(was a member, is a member now)` from the log and the last snapshot.
    ///
    /// A removal appends a membership that drops this id. An earlier entry still
    /// contains it. A joiner that has not received its own add entry has neither.
    pub async fn membership_fate(&self, id: u64) -> (bool, bool) {
        let inner = self.inner.read().await;
        let mut was = inner.machine.member_ids.contains(&id);
        let mut latest = None;
        let stored = inner.machine.last_membership.membership();
        if stored.nodes().next().is_some() {
            let contains = contains_id(stored, id);
            was |= contains;
            latest = Some(contains);
        }
        for entry in inner.log.values() {
            if let EntryPayload::Membership(membership) = &entry.payload {
                let contains = contains_id(membership, id);
                was |= contains;
                latest = Some(contains);
            }
        }
        (was, latest.unwrap_or(false))
    }

    /// True when a snapshot hook can take the write lock. A hook that runs
    /// while `build_snapshot` holds that lock times out here.
    pub async fn write_permit_available(&self) -> bool {
        tokio::time::timeout(std::time::Duration::from_millis(200), self.inner.write())
            .await
            .is_ok()
    }

    /// Sync the idempotency map from the state-machine worker.
    /// The raft core does not wait on this file.
    async fn flush_machine(&self) -> Result<(), StorageError<NodeId>> {
        let (dir, bytes) = {
            let inner = self.inner.read().await;
            let Some(dir) = inner.dir.clone() else {
                return Ok(());
            };
            let bytes = serde_json::to_vec(&inner.machine).map_err(disk_err)?;
            (dir, bytes)
        };
        blocking_disk(move || atomic_write(&dir, "machine.json", &bytes)).await
    }

    async fn flush_meta(&self) -> Result<(), StorageError<NodeId>> {
        let _guard = self.meta_lock.lock().await;
        let (dir, bytes) = {
            let inner = self.inner.read().await;
            let Some(dir) = inner.dir.clone() else {
                return Ok(());
            };
            let bytes = serde_json::to_vec(&inner.meta_file()).map_err(disk_err)?;
            (dir, bytes)
        };
        blocking_disk(move || atomic_write(&dir, "meta.json", &bytes)).await
    }
}

enum LogJob {
    Append(Option<(PathBuf, u64, Vec<u8>)>),
    Rewrite(Option<(PathBuf, Vec<u8>)>),
}

async fn write_log_job(job: LogJob) -> Result<(), StorageError<NodeId>> {
    match job {
        LogJob::Append(Some((dir, offset, bytes))) => {
            blocking_disk(move || append_at(&dir.join("log.dat"), offset, &bytes)).await
        }
        LogJob::Rewrite(Some((dir, bytes))) => {
            blocking_disk(move || atomic_write(&dir, "log.dat", &bytes)).await
        }
        LogJob::Append(None) | LogJob::Rewrite(None) => Ok(()),
    }
}

impl Default for SqlStore {
    fn default() -> Self {
        Self::new()
    }
}

impl StoreInner {
    fn empty(dir: Option<PathBuf>) -> Self {
        Self {
            dir,
            last_purged_log_id: None,
            log: BTreeMap::new(),
            log_bytes: 0,
            log_durable_index: None,
            log_v2: true,
            machine: Machine::default(),
            vote: None,
            snapshot_idx: 0,
            disk_snapshot_idx: 0,
            snapshot_meta: None,
            memory_snapshot: None,
        }
    }

    fn load(dir: &Path) -> Result<Self, std::io::Error> {
        let legacy = read_json::<DiskState>(&dir.join("state.json"))?;
        let vote = read_json::<Vote<NodeId>>(&dir.join("vote.json"))?
            .or_else(|| legacy.as_ref().and_then(|state| state.vote));
        let meta = read_json::<MetaFile>(&dir.join("meta.json"))?;
        let mut machine = read_json::<Machine>(&dir.join("machine.json"))?.unwrap_or_else(|| {
            legacy
                .as_ref()
                .map(|state| state.machine.clone())
                .unwrap_or_default()
        });
        let last_purged_log_id = meta
            .as_ref()
            .and_then(|meta| meta.last_purged_log_id)
            .or_else(|| legacy.as_ref().and_then(|state| state.last_purged_log_id));
        let snapshot_idx = meta
            .as_ref()
            .map(|meta| meta.snapshot_idx)
            .or_else(|| legacy.as_ref().map(|state| state.snapshot_idx))
            .unwrap_or(0);
        let snapshot_meta = meta
            .as_ref()
            .and_then(|meta| meta.snapshot_meta.clone())
            .or_else(|| {
                legacy
                    .as_ref()
                    .and_then(|state| state.snapshot_meta.clone())
            });
        let (entries, valid_len, log_v2) = read_log(&dir.join("log.dat"))?;
        let log_path = dir.join("log.dat");
        if log_path.is_file() {
            let file_len = fs::metadata(&log_path)?.len();
            if file_len > valid_len {
                let file = OpenOptions::new().write(true).open(&log_path)?;
                file.set_len(valid_len)?;
            }
        }
        let log_durable_index = entries.last().map(|entry| entry.log_id.index);
        let mut log = BTreeMap::new();
        let applied_upto = machine.last_applied_log.map(|log_id| log_id.index);
        for entry in entries {
            let purged = last_purged_log_id
                .map(|log_id| entry.log_id.index <= log_id.index)
                .unwrap_or(false);
            if !purged {
                let applied = applied_upto
                    .map(|index| entry.log_id.index <= index)
                    .unwrap_or(false);
                if applied {
                    if let EntryPayload::Membership(membership) = &entry.payload {
                        machine.note(membership);
                    }
                    if let EntryPayload::Normal(command) = &entry.payload {
                        remember_applied(&mut machine, command);
                    }
                }
                log.insert(entry.log_id.index, entry);
            }
        }
        let current = machine.last_membership.membership().clone();
        machine.note(&current);
        if snapshot_meta.is_some() && !dir.join("snapshot.bin").is_file() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "snapshot.bin",
            ));
        }
        Ok(Self {
            dir: Some(dir.to_path_buf()),
            last_purged_log_id,
            log,
            log_bytes: valid_len,
            log_durable_index,
            log_v2,
            machine,
            vote,
            snapshot_idx,
            disk_snapshot_idx: snapshot_idx,
            snapshot_meta,
            memory_snapshot: None,
        })
    }

    fn meta_file(&self) -> MetaFile {
        MetaFile {
            last_purged_log_id: self.last_purged_log_id,
            snapshot_idx: self.snapshot_idx,
            snapshot_meta: self.snapshot_meta.clone(),
        }
    }

    /// Bytes for entries that are in memory and not yet in `log.dat`.
    /// Updates the in-memory offset. The caller writes the bytes.
    fn plan_append(&mut self) -> Result<Option<(PathBuf, u64, Vec<u8>)>, StorageError<NodeId>> {
        let Some(dir) = self.dir.clone() else {
            return Ok(None);
        };
        let start = self
            .log_durable_index
            .map(|index| index.saturating_add(1))
            .unwrap_or(0);
        let mut bytes = Vec::new();
        if self.log_bytes == 0 {
            bytes.extend_from_slice(LOG_MAGIC);
        }
        let mut last = self.log_durable_index;
        for (index, entry) in self.log.range(start..) {
            bytes.extend(encode_entry(entry)?);
            last = Some(*index);
        }
        if last == self.log_durable_index {
            return Ok(None);
        }
        let offset = self.log_bytes;
        self.log_bytes += bytes.len() as u64;
        self.log_durable_index = last;
        self.log_v2 = true;
        Ok(Some((dir, offset, bytes)))
    }

    /// Full replacement of `log.dat` after a truncate or a purge.
    fn plan_rewrite(&mut self) -> Result<Option<(PathBuf, Vec<u8>)>, StorageError<NodeId>> {
        let Some(dir) = self.dir.clone() else {
            return Ok(None);
        };
        let mut bytes = Vec::from(LOG_MAGIC);
        let mut last = None;
        for (index, entry) in &self.log {
            bytes.extend(encode_entry(entry)?);
            last = Some(*index);
        }
        self.log_bytes = bytes.len() as u64;
        self.log_durable_index = last;
        self.log_v2 = true;
        Ok(Some((dir, bytes)))
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
        let hook = self.snapshot_hook.read().expect("snapshot hook").clone();
        let (snapshot_idx, dir) = {
            let inner = self.inner.read().await;
            (inner.snapshot_idx.saturating_add(1), inner.dir.clone())
        };
        // The page copy runs outside the store lock. The raft core appends
        // and sends heartbeats while SQLite copies pages.
        let pages = match &hook {
            Some(hook) => {
                let path = match &dir {
                    Some(dir) => dir.join("snapshot.pages"),
                    None => std::env::temp_dir().join(format!(
                        "osd1-snap-pages-{}-{}",
                        std::process::id(),
                        snapshot_idx
                    )),
                };
                let _ = fs::remove_file(&path);
                hook(path.clone()).await.map_err(|error| {
                    StorageError::from(StorageIOError::read_snapshot(
                        None,
                        &std::io::Error::other(error),
                    ))
                })?;
                Some(path)
            }
            None => None,
        };
        let machine = self.inner.read().await.machine.clone();
        let snapshot_id = match machine.last_applied_log {
            Some(log_id) => format!("{}-{}", log_id.index, snapshot_idx),
            None => format!("empty-{snapshot_idx}"),
        };
        let meta = SnapshotMeta {
            last_log_id: machine.last_applied_log,
            last_membership: machine.last_membership.clone(),
            snapshot_id,
        };
        let data = if let Some(dir) = dir.clone() {
            if let Some(pages) = pages.clone() {
                let dir_for_write = dir.clone();
                let machine_for_write = machine.clone();
                blocking_disk(move || {
                    write_snapshot_bin(&dir_for_write, &machine_for_write, &pages)
                })
                .await?;
                let bin = dir.join("snapshot.bin");
                blocking_disk(move || fs::read(bin).map_err(disk_err)).await?
            } else {
                let data = serde_json::to_vec(&machine).map_err(disk_err)?;
                let bytes = data.clone();
                blocking_disk(move || atomic_write(&dir, "snapshot.bin", &bytes)).await?;
                data
            }
        } else if let Some(pages) = &pages {
            read_pages_snapshot(&machine, pages)?
        } else {
            serde_json::to_vec(&machine).map_err(disk_err)?
        };
        if let Some(pages) = &pages {
            let _ = fs::remove_file(pages);
        }
        {
            let mut inner = self.inner.write().await;
            inner.snapshot_idx = snapshot_idx;
            inner.disk_snapshot_idx = snapshot_idx;
            inner.snapshot_meta = Some(meta.clone());
            inner.memory_snapshot = if dir.is_some() {
                None
            } else {
                Some(data.clone())
            };
        }
        self.flush_meta().await?;
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
        let dir = {
            let mut inner = self.inner.write().await;
            inner.vote = Some(*vote);
            inner.dir.clone()
        };
        if let Some(dir) = dir {
            let bytes = serde_json::to_vec(vote).map_err(disk_err)?;
            blocking_disk(move || atomic_write(&dir, "vote.json", &bytes)).await?;
        }
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
        let batch: Vec<_> = entries.into_iter().collect();
        if batch.is_empty() {
            return Ok(());
        }
        let min_index = batch.iter().map(|entry| entry.log_id.index).min().unwrap();
        let (job, log_bytes, log_durable_index, log_v2) = {
            let mut inner = self.inner.write().await;
            let overlap = inner
                .log_durable_index
                .map(|index| min_index <= index)
                .unwrap_or(false);
            for entry in batch {
                inner.log.insert(entry.log_id.index, entry);
            }
            let log_bytes = inner.log_bytes;
            let log_durable_index = inner.log_durable_index;
            let log_v2 = inner.log_v2;
            let job = if overlap || !inner.log_v2 {
                LogJob::Rewrite(inner.plan_rewrite()?)
            } else {
                LogJob::Append(inner.plan_append()?)
            };
            (job, log_bytes, log_durable_index, log_v2)
        };
        let rewrite = matches!(job, LogJob::Rewrite(_));
        if let Err(error) = write_log_job(job).await {
            let mut inner = self.inner.write().await;
            inner.log_bytes = log_bytes;
            inner.log_durable_index = log_durable_index;
            // A failed rewrite can leave a newer entry on an index the file
            // still has. Clearing log_v2 makes the next append rewrite it.
            inner.log_v2 = if rewrite { false } else { log_v2 };
            return Err(error);
        }
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        let prepared = {
            let mut inner = self.inner.write().await;
            let mut removed = inner.log.split_off(&log_id.index);
            let log_bytes = inner.log_bytes;
            let log_durable_index = inner.log_durable_index;
            let log_v2 = inner.log_v2;
            match inner.plan_rewrite() {
                Ok(planned) => Ok((
                    LogJob::Rewrite(planned),
                    removed,
                    log_bytes,
                    log_durable_index,
                    log_v2,
                )),
                Err(error) => {
                    // `removed` starts at `log_id.index`, so its keys are greater.
                    inner.log.append(&mut removed);
                    inner.log_bytes = log_bytes;
                    inner.log_durable_index = log_durable_index;
                    inner.log_v2 = log_v2;
                    Err(error)
                }
            }
        };
        let (job, mut removed, log_bytes, log_durable_index, log_v2) = prepared?;
        if let Err(error) = write_log_job(job).await {
            let mut inner = self.inner.write().await;
            inner.log.append(&mut removed);
            inner.log_bytes = log_bytes;
            inner.log_durable_index = log_durable_index;
            inner.log_v2 = log_v2;
            return Err(error);
        }
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let prepared = {
            let mut inner = self.inner.write().await;
            debug_assert!(inner.last_purged_log_id <= Some(log_id));
            let previous_purged = inner.last_purged_log_id;
            inner.last_purged_log_id = Some(log_id);
            let mut prefix = std::mem::take(&mut inner.log);
            inner.log = prefix.split_off(&(log_id.index.saturating_add(1)));
            let log_bytes = inner.log_bytes;
            let log_durable_index = inner.log_durable_index;
            let log_v2 = inner.log_v2;
            match inner.plan_rewrite() {
                Ok(planned) => Ok((
                    LogJob::Rewrite(planned),
                    prefix,
                    log_bytes,
                    log_durable_index,
                    log_v2,
                    previous_purged,
                )),
                Err(error) => {
                    // `prefix` holds indexes at or below `log_id`, so its keys are smaller.
                    prefix.append(&mut inner.log);
                    inner.log = prefix;
                    inner.log_bytes = log_bytes;
                    inner.log_durable_index = log_durable_index;
                    inner.log_v2 = log_v2;
                    inner.last_purged_log_id = previous_purged;
                    Err(error)
                }
            }
        };
        let (job, mut prefix, log_bytes, log_durable_index, log_v2, previous_purged) = prepared?;
        if let Err(error) = write_log_job(job).await {
            let mut inner = self.inner.write().await;
            prefix.append(&mut inner.log);
            inner.log = prefix;
            inner.log_bytes = log_bytes;
            inner.log_durable_index = log_durable_index;
            inner.log_v2 = log_v2;
            inner.last_purged_log_id = previous_purged;
            return Err(error);
        }
        self.flush_meta().await?;
        let hook = self.body_horizon_hook.read().expect("body horizon").clone();
        if let Some(hook) = hook {
            hook(log_id.index).await;
        }
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
        let memory_only = self.inner.read().await.dir.is_none();
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
                        if memory_only {
                            let key = seen_key(command.origin, command.id);
                            if let Some(body) = inner.machine.seen.get(&key) {
                                planned.push(Plan::Done(SqlResponse {
                                    duplicate: true,
                                    body_json: body.clone(),
                                    commit_index: 0,
                                }));
                                continue;
                            }
                            // Remember the key before the hook returns so a second
                            // delivery in this process does not run the SQL twice.
                            inner.machine.seen.insert(key, String::new());
                        }
                        remember_applied(&mut inner.machine, command);
                        let mut command = command.clone();
                        command.log_index = entry.log_id.index;
                        planned.push(Plan::Run(command));
                    }
                    EntryPayload::Membership(membership) => {
                        inner.machine.note(membership);
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
                    if memory_only {
                        let key = seen_key(command.origin, command.id);
                        self.inner
                            .write()
                            .await
                            .machine
                            .seen
                            .insert(key, body_json.clone());
                    }
                    responses.push(SqlResponse {
                        duplicate: false,
                        body_json,
                        commit_index: 0,
                    });
                }
            }
        }
        self.flush_machine().await?;
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
        let hook = self.install_hook.read().expect("install hook").clone();
        let machine = if bytes.starts_with(SNAP_MAGIC) {
            let (machine, sqlite) = decode_snapshot(&bytes).map_err(disk_err)?;
            if let Some(hook) = hook {
                hook(sqlite).await.map_err(|error| {
                    StorageError::from(StorageIOError::write_snapshot(
                        Some(meta.signature()),
                        &std::io::Error::other(error),
                    ))
                })?;
            }
            machine
        } else {
            serde_json::from_slice(&bytes).map_err(disk_err)?
        };
        let (dir, snapshot_idx) = {
            let inner = self.inner.read().await;
            (inner.dir.clone(), inner.snapshot_idx.saturating_add(1))
        };
        let stored = if let Some(dir) = dir {
            blocking_disk(move || atomic_write(&dir, "snapshot.bin", &bytes)).await?;
            None
        } else {
            Some(bytes)
        };
        {
            let mut inner = self.inner.write().await;
            inner.machine = machine;
            let membership = inner.machine.last_membership.membership().clone();
            inner.machine.note(&membership);
            inner.snapshot_meta = Some(meta.clone());
            inner.memory_snapshot = stored;
            inner.snapshot_idx = snapshot_idx;
            inner.disk_snapshot_idx = snapshot_idx;
        }
        self.flush_machine().await?;
        self.flush_meta().await?;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let (meta, dir, memory) = {
            let inner = self.inner.read().await;
            (
                inner.snapshot_meta.clone(),
                inner.dir.clone(),
                inner.memory_snapshot.clone(),
            )
        };
        let Some(meta) = meta else {
            return Ok(None);
        };
        let data = if let Some(dir) = dir {
            let path = dir.join("snapshot.bin");
            blocking_disk(move || fs::read(&path).map_err(disk_err)).await?
        } else {
            memory.unwrap_or_default()
        };
        Ok(Some(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        }))
    }
}

fn seen_key(origin: u64, id: u64) -> String {
    format!("{origin}:{id}")
}

fn remember_applied(machine: &mut Machine, command: &SqlCommand) {
    let exists = machine
        .applied
        .iter()
        .any(|item| item.origin == command.origin && item.id == command.id);
    if !exists {
        machine.applied.push(command.clone());
    }
}

fn contains_id(membership: &openraft::Membership<NodeId, BasicNode>, id: u64) -> bool {
    membership.nodes().any(|(node_id, _)| *node_id == id)
}

fn empty_response(duplicate: bool) -> SqlResponse {
    SqlResponse {
        duplicate,
        body_json: String::new(),
        commit_index: 0,
    }
}

fn encode_snapshot(machine: &Machine, sqlite: &[u8]) -> Result<Vec<u8>, serde_json::Error> {
    let json = serde_json::to_vec(machine)?;
    let mut out = Vec::with_capacity(SNAP_MAGIC.len() + 8 + json.len() + sqlite.len());
    out.extend_from_slice(SNAP_MAGIC);
    out.extend_from_slice(&(json.len() as u64).to_le_bytes());
    out.extend_from_slice(&json);
    out.extend_from_slice(sqlite);
    Ok(out)
}

fn decode_snapshot(bytes: &[u8]) -> Result<(Machine, Vec<u8>), serde_json::Error> {
    let rest = bytes.get(SNAP_MAGIC.len()..).unwrap_or_default();
    if rest.len() < 8 {
        return Err(serde::de::Error::custom("snapshot header"));
    }
    let len = u64::from_le_bytes(rest[..8].try_into().unwrap_or([0; 8])) as usize;
    let json_end = 8 + len;
    if rest.len() < json_end {
        return Err(serde::de::Error::custom("snapshot machine"));
    }
    let sqlite = rest[json_end..].to_vec();
    Ok((serde_json::from_slice(&rest[8..json_end])?, sqlite))
}

fn disk_err(error: impl std::fmt::Display) -> StorageError<NodeId> {
    StorageError::from(StorageIOError::write_logs(&std::io::Error::other(
        error.to_string(),
    )))
}

fn read_log(path: &Path) -> Result<(Vec<Entry<TypeConfig>>, u64, bool), std::io::Error> {
    if !path.exists() {
        return Ok((Vec::new(), 0, true));
    }
    let bytes = fs::read(path)?;
    if bytes.is_empty() {
        return Ok((Vec::new(), 0, true));
    }
    if bytes.starts_with(LOG_MAGIC) {
        return read_log_v2(&bytes);
    }
    let (entries, valid_len) = read_log_legacy(&bytes)?;
    Ok((entries, valid_len, false))
}

fn read_log_v2(bytes: &[u8]) -> Result<(Vec<Entry<TypeConfig>>, u64, bool), std::io::Error> {
    let mut entries = Vec::new();
    let mut offset = LOG_MAGIC.len();
    while offset + 8 <= bytes.len() {
        let len = u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        let crc = u32::from_be_bytes(bytes[offset + 4..offset + 8].try_into().unwrap());
        let start = offset + 8;
        let end = start + len;
        if end > bytes.len() {
            break;
        }
        let payload = &bytes[start..end];
        if crc32_ieee(payload) != crc {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "raft log checksum",
            ));
        }
        let entry = serde_json::from_slice(payload)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        entries.push(entry);
        offset = end;
    }
    Ok((entries, offset as u64, true))
}

fn read_log_legacy(bytes: &[u8]) -> Result<(Vec<Entry<TypeConfig>>, u64), std::io::Error> {
    let mut entries = Vec::new();
    let mut offset = 0;
    while offset + 4 <= bytes.len() {
        let len = u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        let start = offset + 4;
        let end = start + len;
        if end > bytes.len() {
            break;
        }
        let entry = serde_json::from_slice(&bytes[start..end])
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        entries.push(entry);
        offset = end;
    }
    Ok((entries, offset as u64))
}

fn crc32_ieee(data: &[u8]) -> u32 {
    let mut crc = 0xffff_ffffu32;
    for &byte in data {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xedb8_8320 & mask);
        }
    }
    !crc
}

fn read_json<T: DeserializeOwned>(path: &Path) -> Result<Option<T>, std::io::Error> {
    if !path.is_file() {
        return Ok(None);
    }
    let bytes = fs::read(path)?;
    let value = serde_json::from_slice(&bytes)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    Ok(Some(value))
}

fn encode_entry(entry: &Entry<TypeConfig>) -> Result<Vec<u8>, StorageError<NodeId>> {
    let entry_bytes = serde_json::to_vec(entry).map_err(disk_err)?;
    let len = u32::try_from(entry_bytes.len()).map_err(disk_err)?;
    let crc = crc32_ieee(&entry_bytes);
    let mut out = Vec::with_capacity(8 + entry_bytes.len());
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(&crc.to_be_bytes());
    out.extend_from_slice(&entry_bytes);
    Ok(out)
}

fn write_snapshot_bin(
    dir: &Path,
    machine: &Machine,
    pages: &Path,
) -> Result<(), StorageError<NodeId>> {
    let json = serde_json::to_vec(machine).map_err(disk_err)?;
    let tmp = dir.join("snapshot.bin.tmp");
    let final_path = dir.join("snapshot.bin");
    {
        let mut file = File::create(&tmp).map_err(disk_err)?;
        file.write_all(SNAP_MAGIC).map_err(disk_err)?;
        file.write_all(&(json.len() as u64).to_le_bytes())
            .map_err(disk_err)?;
        file.write_all(&json).map_err(disk_err)?;
        let mut input = File::open(pages).map_err(disk_err)?;
        std::io::copy(&mut input, &mut file).map_err(disk_err)?;
        file.sync_all().map_err(disk_err)?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(&tmp, fs::Permissions::from_mode(0o600));
    }
    fs::rename(&tmp, &final_path).map_err(disk_err)?;
    if let Ok(dir_file) = File::open(dir) {
        let _ = dir_file.sync_all();
    }
    Ok(())
}

fn read_pages_snapshot(machine: &Machine, pages: &Path) -> Result<Vec<u8>, StorageError<NodeId>> {
    let sqlite = fs::read(pages).map_err(disk_err)?;
    encode_snapshot(machine, &sqlite).map_err(disk_err)
}

async fn blocking_disk<T: Send + 'static>(
    job: impl FnOnce() -> Result<T, StorageError<NodeId>> + Send + 'static,
) -> Result<T, StorageError<NodeId>> {
    match tokio::task::spawn_blocking(job).await {
        Ok(result) => result,
        Err(error) => Err(disk_err(error)),
    }
}

fn append_at(path: &Path, offset: u64, bytes: &[u8]) -> Result<(), StorageError<NodeId>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(disk_err)?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(path)
        .map_err(disk_err)?;
    let len = file.metadata().map_err(disk_err)?.len();
    if len > offset {
        file.set_len(offset).map_err(disk_err)?;
    }
    file.seek(SeekFrom::Start(offset)).map_err(disk_err)?;
    file.write_all(bytes).map_err(disk_err)?;
    file.sync_all().map_err(disk_err)?;
    if offset == 0 {
        if let Some(dir) = path.parent() {
            if let Ok(dir_file) = File::open(dir) {
                let _ = dir_file.sync_all();
            }
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o600));
    }
    Ok(())
}

fn atomic_write(dir: &Path, name: &str, bytes: &[u8]) -> Result<(), StorageError<NodeId>> {
    let tmp = dir.join(format!("{name}.tmp"));
    let final_path = dir.join(name);
    let mut file = File::create(&tmp).map_err(disk_err)?;
    file.write_all(bytes).map_err(disk_err)?;
    file.sync_all().map_err(disk_err)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(&tmp, fs::Permissions::from_mode(0o600));
    }
    fs::rename(&tmp, &final_path).map_err(disk_err)?;
    if let Ok(dir_file) = File::open(dir) {
        let _ = dir_file.sync_all();
    }
    Ok(())
}

fn create_private_dir(path: &Path) -> std::io::Result<()> {
    fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

/// Directory that belongs to `db_path`. `./data/node1.db` keeps its log in `./data/node1.raft`.
pub fn raft_dir(db_path: &Path) -> PathBuf {
    db_path.with_extension("raft")
}

/// True when a restart should keep this directory instead of bootstrapping or joining.
pub fn raft_dir_has_state(dir: &Path) -> bool {
    [
        "state.json",
        "vote.json",
        "log.dat",
        "machine.json",
        "meta.json",
    ]
    .iter()
    .any(|name| dir.join(name).is_file())
}
