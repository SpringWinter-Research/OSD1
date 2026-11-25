//! Executable checks of the Raft paper's Figure 3 properties.
//!
//! These tests run an in-process openraft 0.9 cluster. They are not a TLA+
//! proof, and they do not model-check openraft itself. Each test forces one
//! property and fails if this crate's use of openraft breaks it:
//!
//! - Election Safety: at most one leader per term.
//! - Leader Completeness: a committed entry is on the next leader.
//! - State Machine Safety: applied command lists match.
//! - Quorum: a minority cannot commit.

use std::collections::HashMap;
use std::time::Duration;

use openraft::testing::Suite;

use crate::Peers;
use crate::RaftNode;
use crate::SqlCommand;
use crate::SqlStore;
use crate::TypeConfig;

fn command(id: u64, sql: &str) -> SqlCommand {
    SqlCommand {
        origin: 1,
        id,
        sql: sql.to_string(),
        params_json: "[]".to_string(),
        log_index: 0,
    }
}

async fn form(ids: &[u64]) -> Vec<RaftNode> {
    let peers = Peers::new();
    let mut nodes = Vec::with_capacity(ids.len());
    for id in ids {
        nodes.push(
            RaftNode::start(*id, peers.clone(), SqlStore::new())
                .await
                .expect("start"),
        );
    }
    nodes[0].bootstrap().await.expect("bootstrap");
    for id in ids.iter().skip(1) {
        nodes[0]
            .add_voter(*id, id.to_string())
            .await
            .expect("add voter");
    }
    for node in &nodes {
        node.wait_until(
            |status| status.leader.is_some() && status.role != "learner",
            Duration::from_secs(5),
        )
        .await
        .expect("node joined");
    }
    nodes
}

async fn wait_command(nodes: &[RaftNode], id: u64) {
    for node in nodes {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if node.applied().await.iter().any(|command| command.id == id) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "node {} never applied {id}",
                node.id()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

async fn leader(nodes: &[RaftNode]) -> RaftNode {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    let mut last;
    loop {
        let mut lines = Vec::new();
        for node in nodes {
            let status = node.status().await.expect("status");
            lines.push(format!("{}={}", status.id, status.role));
            if status.role == "leader" {
                return node.clone();
            }
        }
        last = lines.join(" ");
        assert!(tokio::time::Instant::now() < deadline, "no leader ({last})");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn commands_of(nodes: &[Vec<SqlCommand>]) -> Vec<Vec<u64>> {
    nodes
        .iter()
        .map(|commands| commands.iter().map(|command| command.id).collect())
        .collect()
}

#[tokio::test]
async fn three_nodes_commit_and_converge() {
    let nodes = form(&[1, 2, 3]).await;
    let lead = leader(&nodes).await;
    let response = lead
        .propose(command(1, "create table t(n integer)"))
        .await
        .expect("commit");
    assert!(!response.duplicate);
    assert!(response.body_json.is_empty());
    wait_command(&nodes, 1).await;

    let mut applied = Vec::new();
    let mut commit = None;
    for node in &nodes {
        let commands = node.applied().await;
        let status = node.status().await.expect("status");
        assert_eq!(status.commit_index, status.apply_index);
        commit = Some(status.commit_index);
        applied.push(commands);
    }
    assert!(commit.unwrap().unwrap() >= 1);
    assert_eq!(commands_of(&applied), vec![vec![1], vec![1], vec![1]]);
    for node in &nodes {
        node.shutdown().await.expect("shutdown");
    }
}

#[tokio::test]
async fn election_safety_one_leader_per_term() {
    let nodes = form(&[1, 2, 3]).await;
    let transport = nodes[0].transport();
    let mut leader_by_term: HashMap<u64, u64> = HashMap::new();

    for _ in 0..3 {
        let current = leader(&nodes).await;
        let current_id = current.id();
        transport.isolate(current_id).await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            for node in &nodes {
                let status = node.status().await.expect("status");
                if status.role == "leader" {
                    if let Some(previous) = leader_by_term.insert(status.term, node.id()) {
                        assert_eq!(
                            previous,
                            node.id(),
                            "term {} elected both {previous} and {}",
                            status.term,
                            node.id()
                        );
                    }
                }
            }
            let other_is_leader = {
                let mut found = false;
                for node in &nodes {
                    if node.id() == current_id {
                        continue;
                    }
                    if node.status().await.expect("status").role == "leader" {
                        found = true;
                    }
                }
                found
            };
            if other_is_leader || tokio::time::Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        transport.heal(current_id).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    assert!(
        leader_by_term.len() >= 2,
        "expected a new term after isolating the leader, saw {leader_by_term:?}"
    );
    for node in &nodes {
        node.shutdown().await.expect("shutdown");
    }
}

#[tokio::test]
async fn committed_entry_survives_leader_loss() {
    let nodes = form(&[1, 2, 3]).await;
    let lead = leader(&nodes).await;
    lead.propose(command(1, "insert into t values (1)"))
        .await
        .expect("commit");
    wait_command(&nodes, 1).await;

    let lost = lead.id();
    lead.shutdown().await.expect("stop leader");
    let remaining: Vec<_> = nodes
        .iter()
        .filter(|node| node.id() != lost)
        .cloned()
        .collect();
    let next = leader(&remaining).await;
    for node in &remaining {
        assert!(
            node.applied().await.iter().any(|command| command.id == 1),
            "node {} lost a committed command",
            node.id()
        );
    }
    next.propose(command(2, "insert into t values (2)"))
        .await
        .expect("commit after election");
    wait_command(&remaining, 2).await;
    for node in &remaining {
        let ids: Vec<_> = node
            .applied()
            .await
            .iter()
            .map(|command| command.id)
            .collect();
        assert_eq!(ids, vec![1, 2]);
    }
    for node in &remaining {
        node.shutdown().await.expect("shutdown");
    }
}

#[tokio::test]
async fn minority_cannot_commit() {
    let nodes = form(&[1, 2, 3]).await;
    let lead = leader(&nodes).await;
    lead.propose(command(1, "insert into t values (1)"))
        .await
        .expect("commit");
    wait_command(&nodes, 1).await;

    for node in &nodes {
        if node.id() != lead.id() {
            node.shutdown().await.expect("stop peer");
        }
    }
    let write = tokio::time::timeout(
        Duration::from_millis(1200),
        lead.propose(command(2, "insert into t values (2)")),
    )
    .await;
    assert!(
        write.is_err() || write.unwrap().is_err(),
        "a single node of three committed a write"
    );
    assert!(
        !lead.applied().await.iter().any(|command| command.id == 2),
        "minority applied the uncommitted command"
    );
    lead.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn two_nodes_need_both_to_commit() {
    let nodes = form(&[1, 2]).await;
    let lead = leader(&nodes).await;
    lead.propose(command(1, "insert into t values (1)"))
        .await
        .expect("both nodes stored it");
    wait_command(&nodes, 1).await;
    for node in &nodes {
        if node.id() != lead.id() {
            node.shutdown().await.expect("stop the other voter");
        }
    }
    let write = tokio::time::timeout(
        Duration::from_millis(1200),
        lead.propose(command(2, "insert into t values (2)")),
    )
    .await;
    assert!(
        write.is_err() || write.unwrap().is_err(),
        "one node of a pair committed alone"
    );
    assert!(!lead.applied().await.iter().any(|command| command.id == 2));
    lead.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn idempotent_apply() {
    let nodes = form(&[1, 2, 3]).await;
    let lead = leader(&nodes).await;
    let first = lead
        .propose(command(7, "insert into t values (7)"))
        .await
        .expect("first");
    assert!(!first.duplicate);
    let second = lead
        .propose(command(7, "insert into t values (7)"))
        .await
        .expect("retry");
    assert!(second.duplicate);
    wait_command(&nodes, 7).await;
    for node in &nodes {
        let matches = node
            .applied()
            .await
            .iter()
            .filter(|command| command.id == 7)
            .count();
        assert_eq!(matches, 1);
    }
    let follower = nodes.iter().find(|node| node.id() != lead.id()).unwrap();
    let rejected = follower
        .propose(command(8, "insert into t values (8)"))
        .await;
    assert!(matches!(
        rejected,
        Err(crate::ProposeError::NotLeader { .. })
    ));
    for node in &nodes {
        node.shutdown().await.expect("shutdown");
    }
}

#[tokio::test]
async fn thirty_writes_replicate() {
    let nodes = form(&[1, 2, 3]).await;
    let lead = leader(&nodes).await;
    let started = std::time::Instant::now();
    for id in 1..=30 {
        lead.propose(command(id, "insert into t values (1)"))
            .await
            .expect("commit");
    }
    let elapsed = started.elapsed();
    wait_command(&nodes, 30).await;
    for node in &nodes {
        assert_eq!(node.applied().await.len(), 30);
    }
    eprintln!(
        "osd1-raft 30 commits on 3 in-process nodes: {:.3} ms",
        elapsed.as_secs_f64() * 1000.0
    );
    for node in &nodes {
        node.shutdown().await.expect("shutdown");
    }
}

#[tokio::test]
async fn learners_do_not_change_quorum() {
    let peers = Peers::new();
    let leader = RaftNode::start(1, peers.clone(), SqlStore::new())
        .await
        .expect("start");
    leader.bootstrap().await.expect("bootstrap");
    let reader = RaftNode::start(2, peers.clone(), SqlStore::new())
        .await
        .expect("reader");
    leader.add_learner(2, "2").await.expect("learner");
    reader
        .wait_until(|status| status.role == "learner", Duration::from_secs(5))
        .await
        .expect("reader is a learner");
    let reader_status = reader.status().await.expect("reader status");
    assert_eq!(
        reader_status.peers, 2,
        "learner must count itself: {reader_status:?}"
    );
    let leader_status = leader.status().await.expect("leader status");
    assert_eq!(
        leader_status.peers, reader_status.peers,
        "{leader_status:?}"
    );
    peers.isolate(2).await;
    leader
        .propose(command(1, "create table t(n integer)"))
        .await
        .expect("one voter still commits");
    leader.shutdown().await.expect("shutdown");
    reader.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn removed_voter_leaves_the_membership() {
    let nodes = form(&[1, 2, 3]).await;
    let lead = leader(&nodes).await;
    lead.remove_member(3).await.expect("remove");
    let status = lead.status().await.expect("status");
    assert_eq!(status.peers, 2, "{status:?}");
    lead.propose(command(1, "create table t(n integer)"))
        .await
        .expect("two voters still commit");
    for node in &nodes {
        node.shutdown().await.expect("shutdown");
    }
}

#[tokio::test]
async fn parallel_voter_joins_both_commit() {
    let peers = Peers::new();
    let mut nodes = Vec::new();
    for id in [1, 2, 3] {
        nodes.push(
            RaftNode::start(id, peers.clone(), SqlStore::new())
                .await
                .expect("start"),
        );
    }
    nodes[0].bootstrap().await.expect("bootstrap");
    let lead = nodes[0].clone();
    let (first, second) = tokio::join!(lead.add_voter(2, "2"), lead.add_voter(3, "3"));
    first.expect("voter 2");
    second.expect("voter 3");
    let status = nodes[0].status().await.expect("status");
    assert_eq!(status.peers, 3, "{status:?}");
    for node in &nodes {
        node.shutdown().await.expect("shutdown");
    }
}

#[tokio::test]
async fn log_appends_do_not_rewrite_the_prefix() {
    use openraft::storage::RaftStorage;
    use openraft::CommittedLeaderId;
    use openraft::Entry;
    use openraft::EntryPayload;
    use openraft::LogId;

    let dir = std::env::temp_dir().join(format!(
        "osd1-log-append-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let mut store = SqlStore::open(&dir).expect("open");
    let entry = |index: u64| Entry::<TypeConfig> {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
        payload: EntryPayload::Blank,
    };
    store.append_to_log([entry(1)]).await.expect("append 1");
    let first = std::fs::read(dir.join("log.dat")).expect("log");
    store.append_to_log([entry(2)]).await.expect("append 2");
    let second = std::fs::read(dir.join("log.dat")).expect("log");
    assert!(second.starts_with(&first), "append rewrote the log prefix");
    assert!(second.len() > first.len());
    assert!(first.starts_with(b"OSD1LOG2"));
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn a_bad_log_checksum_refuses_to_open() {
    use openraft::storage::RaftStorage;
    use openraft::CommittedLeaderId;
    use openraft::Entry;
    use openraft::EntryPayload;
    use openraft::LogId;

    let dir = std::env::temp_dir().join(format!(
        "osd1-log-crc-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let mut store = SqlStore::open(&dir).expect("open");
    let entry = Entry::<TypeConfig> {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), 1),
        payload: EntryPayload::Blank,
    };
    store.append_to_log([entry]).await.expect("append");
    drop(store);
    let path = dir.join("log.dat");
    let mut bytes = std::fs::read(&path).expect("log");
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    std::fs::write(&path, &bytes).expect("corrupt");
    match SqlStore::open(&dir) {
        Ok(_) => panic!("a bad checksum opened"),
        Err(error) => assert!(error.to_string().contains("checksum"), "{error}"),
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn a_legacy_log_is_rewritten_with_checksums() {
    use openraft::storage::RaftStorage;
    use openraft::CommittedLeaderId;
    use openraft::Entry;
    use openraft::EntryPayload;
    use openraft::LogId;
    use openraft::RaftLogReader;

    let dir = std::env::temp_dir().join(format!(
        "osd1-log-legacy-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");
    let entry = Entry::<TypeConfig> {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), 1),
        payload: EntryPayload::Blank,
    };
    let payload = serde_json::to_vec(&entry).expect("json");
    let mut legacy = Vec::new();
    legacy.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    legacy.extend_from_slice(&payload);
    std::fs::write(dir.join("log.dat"), &legacy).expect("legacy");
    let mut store = SqlStore::open(&dir).expect("open legacy");
    let loaded = store.try_get_log_entries(..).await.expect("entries");
    assert_eq!(loaded.len(), 1);
    let next = Entry::<TypeConfig> {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), 2),
        payload: EntryPayload::Blank,
    };
    store.append_to_log([next]).await.expect("append");
    let rewritten = std::fs::read(dir.join("log.dat")).expect("rewritten");
    assert!(rewritten.starts_with(b"OSD1LOG2"));
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn snapshot_copy_does_not_hold_the_store_lock() {
    let dir = std::env::temp_dir().join(format!(
        "osd1-snap-lock-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let store = SqlStore::open(&dir).expect("open");
    let probe = store.clone();
    store.set_snapshot_hook(std::sync::Arc::new(move |path| {
        let probe = probe.clone();
        Box::pin(async move {
            assert!(
                probe.write_permit_available().await,
                "snapshot held the store write lock"
            );
            std::fs::write(&path, b"pages").map_err(|error| error.to_string())?;
            Ok(())
        })
    }));
    let mut builder = store.clone();
    openraft::storage::RaftSnapshotBuilder::build_snapshot(&mut builder)
        .await
        .expect("snapshot");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn purged_membership_still_records_a_removed_member() {
    use openraft::storage::RaftStorage;
    use openraft::CommittedLeaderId;
    use openraft::Entry;
    use openraft::EntryPayload;
    use openraft::LogId;
    use openraft::Membership;
    use std::collections::BTreeSet;

    let mut store = SqlStore::new();
    let entry = |index: u64, ids: &[u64]| Entry::<TypeConfig> {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
        payload: EntryPayload::Membership(Membership::new(
            vec![BTreeSet::from_iter(ids.iter().copied())],
            (),
        )),
    };
    let first = entry(1, &[1, 2]);
    let first_id = first.log_id;
    store.append_to_log([first.clone()]).await.expect("append");
    store.apply_to_state_machine(&[first]).await.expect("apply");
    store.purge_logs_upto(first_id).await.expect("purge");
    let second = entry(2, &[1]);
    store.append_to_log([second.clone()]).await.expect("append");
    store
        .apply_to_state_machine(&[second])
        .await
        .expect("apply");
    assert_eq!(store.membership_fate(2).await, (true, false));
    assert_eq!(store.membership_fate(1).await, (true, true));
    assert_eq!(store.membership_fate(9).await, (false, false));
}

#[tokio::test]
async fn slow_apply_does_not_start_an_election() {
    let peers = Peers::new();
    let mut nodes = Vec::new();
    for id in [1, 2, 3] {
        let store = SqlStore::new();
        store.set_apply_hook(std::sync::Arc::new(|_command| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(400)).await;
                String::new()
            })
        }));
        nodes.push(
            RaftNode::start(id, peers.clone(), store)
                .await
                .expect("start"),
        );
    }
    nodes[0].bootstrap().await.expect("bootstrap");
    for id in [2, 3] {
        nodes[0].add_voter(id, id.to_string()).await.expect("voter");
    }
    let lead = leader(&nodes).await;
    let term = lead.status().await.expect("status").term;
    lead.propose(command(1, "create table t(n integer)"))
        .await
        .expect("commit during slow apply");
    for node in &nodes {
        let status = node.status().await.expect("status");
        assert_eq!(
            status.term,
            term,
            "node {} changed term: {status:?}",
            node.id()
        );
    }
    for node in &nodes {
        node.shutdown().await.expect("shutdown");
    }
}

#[tokio::test]
async fn durable_log_survives_restart() {
    let root = std::env::temp_dir().join(format!(
        "osd1-raft-disk-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("root");
    let peers = Peers::new();
    let mut nodes = Vec::new();
    for id in [1_u64, 2, 3] {
        let store = SqlStore::open(root.join(id.to_string())).expect("open");
        nodes.push(
            RaftNode::start(id, peers.clone(), store)
                .await
                .expect("start"),
        );
    }
    nodes[0].bootstrap().await.expect("bootstrap");
    nodes[0].add_voter(2, "2").await.expect("voter 2");
    nodes[0].add_voter(3, "3").await.expect("voter 3");
    nodes[0]
        .propose(command(1, "create table t(n integer)"))
        .await
        .expect("commit");
    wait_command(&nodes, 1).await;
    for node in &nodes {
        node.shutdown().await.expect("shutdown");
    }

    let peers = Peers::new();
    let mut nodes = Vec::new();
    for id in [1_u64, 2, 3] {
        let store = SqlStore::open(root.join(id.to_string())).expect("reopen");
        assert!(store.has_state().await, "node {id} lost its log");
        nodes.push(
            RaftNode::start(id, peers.clone(), store)
                .await
                .expect("restart"),
        );
    }
    let lead = leader(&nodes).await;
    assert!(
        lead.applied().await.iter().any(|command| command.id == 1),
        "restart dropped the committed command"
    );
    lead.propose(command(2, "insert into t values (1)"))
        .await
        .expect("commit after restart");
    wait_command(&nodes, 2).await;
    for node in &nodes {
        node.shutdown().await.expect("shutdown");
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn snapshot_carries_sqlite_bytes() {
    let dir = std::env::temp_dir().join(format!("osd1-snap-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let store = SqlStore::open(&dir).expect("open");
    let installed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let installed_hook = std::sync::Arc::clone(&installed);
    store.set_snapshot_hook(std::sync::Arc::new(|path| {
        Box::pin(async move {
            std::fs::write(&path, b"sqlite-pages").map_err(|error| error.to_string())?;
            Ok(())
        })
    }));
    store.set_install_hook(std::sync::Arc::new(move |bytes| {
        let installed_hook = std::sync::Arc::clone(&installed_hook);
        Box::pin(async move {
            *installed_hook.lock().expect("installed") = bytes;
            Ok(())
        })
    }));
    let mut builder = store.clone();
    let snapshot = openraft::storage::RaftSnapshotBuilder::build_snapshot(&mut builder)
        .await
        .expect("snapshot");
    assert!(snapshot
        .snapshot
        .get_ref()
        .windows(9)
        .any(|window| window == b"OSD1SNAP1"));
    let mut target = store.clone();
    openraft::RaftStorage::install_snapshot(&mut target, &snapshot.meta, snapshot.snapshot)
        .await
        .expect("install");
    assert_eq!(
        installed.lock().expect("installed").as_slice(),
        b"sqlite-pages"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Times `append_to_log` on a durable store. Not part of `cargo test`.
/// Run: `cargo test -p osd1-raft --release -- --ignored measure_append_fsync --nocapture`
#[tokio::test]
#[ignore = "disk timing of append_to_log"]
async fn measure_append_fsync() {
    use openraft::storage::RaftStorage;
    use openraft::CommittedLeaderId;
    use openraft::Entry;
    use openraft::EntryPayload;
    use openraft::LogId;

    let dir = std::env::temp_dir().join(format!(
        "osd1-append-fsync-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let mut store = SqlStore::open(&dir).expect("open");
    let entry = |index: u64| Entry::<TypeConfig> {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
        payload: EntryPayload::Blank,
    };
    for index in 1..=5 {
        store.append_to_log([entry(index)]).await.expect("warmup");
    }
    let mut samples = Vec::with_capacity(40);
    for index in 6..=45 {
        let started = std::time::Instant::now();
        store.append_to_log([entry(index)]).await.expect("append");
        samples.push(started.elapsed());
    }
    samples.sort();
    let ms = |sample: std::time::Duration| sample.as_secs_f64() * 1000.0;
    let n = samples.len();
    eprintln!(
        "append_fsync n={n} min={:.3}ms p50={:.3}ms p99={:.3}ms max={:.3}ms",
        ms(samples[0]),
        ms(samples[n / 2]),
        ms(samples[n * 99 / 100]),
        ms(samples[n - 1]),
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn storage_passes_openraft_suite() {
    // Openraft's store suite checks the log, vote, membership, and snapshot
    // contract our Adaptor store has to keep. It is a storage conformance
    // suite, not a proof of the Raft protocol.
    Suite::<TypeConfig, _, _, _, _>::test_all(SqlStore::new_async).expect("openraft store suite");
}
