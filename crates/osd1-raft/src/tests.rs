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

#[test]
fn storage_passes_openraft_suite() {
    // Openraft's store suite checks the log, vote, membership, and snapshot
    // contract our Adaptor store has to keep. It is a storage conformance
    // suite, not a proof of the Raft protocol.
    Suite::<TypeConfig, _, _, _, _>::test_all(SqlStore::new_async).expect("openraft store suite");
}
