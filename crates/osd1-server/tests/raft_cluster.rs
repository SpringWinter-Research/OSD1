//! Three HTTP processes, one raft quorum.
//!
//! These checks run the real server path. They are not a model-checked proof
//! of openraft. Election safety and log matching live in the osd1-raft tests.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use osd1_server::{start_node, NodeConfig, RunningNode};
use serde_json::{json, Value};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_nodes_replicate_proxy_writes_and_export_raft_metrics() {
    let secret = "cluster-secret";
    let token = "raft-token";
    let root = std::env::temp_dir().join(format!("osd1-raft-http-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let leader = node(&root, 1, secret, token, true, None).await;
    let follower = node(
        &root,
        2,
        secret,
        token,
        false,
        Some(leader.raft_addr.to_string()),
    )
    .await;
    let follower_b = node(
        &root,
        3,
        secret,
        token,
        false,
        Some(leader.raft_addr.to_string()),
    )
    .await;

    let (status, body) = post(leader.http_addr, token, "CREATE TABLE t(n INTEGER)", None).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["success"], json!(true), "{body}");
    assert_eq!(body["result"][0]["meta"]["served_by_primary"], json!(true));
    assert_eq!(body["result"][0]["meta"]["served_by_region"], json!("WNAM"));

    let (status, body) = post(
        leader.http_addr,
        token,
        "INSERT INTO t(n) VALUES (?)",
        Some(json!([7])),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert!(
        body["result"][0]["meta"]["last_row_id"]
            .as_i64()
            .unwrap_or(0)
            >= 1
    );

    let seen = wait_row(follower.http_addr, token, 7).await;
    assert_eq!(seen["result"][0]["meta"]["served_by_primary"], json!(false));
    assert_eq!(seen["result"][0]["meta"]["served_by_region"], json!("WNAM"));
    wait_row(follower_b.http_addr, token, 7).await;

    let (status, body) = post(
        follower.http_addr,
        token,
        "INSERT INTO t(n) VALUES (?)",
        Some(json!([8])),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["result"][0]["meta"]["served_by_primary"],
        json!(true),
        "{body}"
    );
    assert!(
        body["result"][0]["meta"]["changes"].as_u64().unwrap_or(0) >= 1,
        "{body}"
    );
    wait_row(follower_b.http_addr, token, 8).await;

    let (status, text) = get(
        follower.http_addr,
        "/accounts/a/d1/database/b/raw",
        token,
        "SELECT n FROM t ORDER BY n",
    )
    .await;
    assert_eq!(status, 200, "{text}");
    let raw: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(raw["result"][0]["results"]["rows"], json!([[7], [8]]));
    assert_eq!(raw["result"][0]["meta"]["served_by_primary"], json!(false));

    let (status, body) = post(leader.http_addr, token, "SELECT datetime('now')", None).await;
    assert_eq!(status, 400, "{body}");
    let message = body["errors"][0]["message"].as_str().unwrap_or("");
    assert!(message.contains("nondeterministic"), "{message}");

    let (status, id_body) = post(leader.http_addr, token, "SELECT last_insert_rowid()", None).await;
    assert_eq!(status, 200, "{id_body}");
    assert_eq!(
        id_body["result"][0]["meta"]["served_by_primary"],
        json!(true)
    );

    let metrics = wait_metrics(leader.http_addr).await;
    assert!(metrics.contains("osd1_raft_role 1"), "{metrics}");
    assert!(metrics.contains("osd1_raft_term"));
    assert!(metrics.contains("osd1_raft_commit_index"));
    assert!(metrics.contains("osd1_raft_apply_index"));
    assert!(metrics.contains("osd1_raft_apply_lag"));
    assert!(metrics.contains("osd1_raft_peers 3"), "{metrics}");
    assert!(!metrics.contains(secret));
    assert!(!metrics.contains("SELECT"));
    let follower_metrics = get_metrics(follower.http_addr).await;
    assert!(
        follower_metrics.contains("osd1_raft_role 0"),
        "{follower_metrics}"
    );

    let refused = osd1_raft::join_cluster(
        &leader.raft_addr.to_string(),
        "nope",
        9,
        "127.0.0.1:1",
        true,
    )
    .await;
    assert!(refused.is_err(), "{refused:?}");

    leader.close().await;
    follower.close().await;
    follower_b.close().await;
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_cluster_serves_the_row_and_commits_again() {
    let secret = "cluster-secret";
    let token = "raft-token";
    let root = std::env::temp_dir().join(format!(
        "osd1-raft-restart-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&root);
    let leader = node(&root, 1, secret, token, true, None).await;
    let join = Some(leader.raft_addr.to_string());
    let follower = node(&root, 2, secret, token, false, join.clone()).await;
    let follower_b = node(&root, 3, secret, token, false, join.clone()).await;

    let (status, body) = post(leader.http_addr, token, "CREATE TABLE t(n INTEGER)", None).await;
    assert_eq!(status, 200, "{body}");
    let (status, body) = post(
        leader.http_addr,
        token,
        "INSERT INTO t(n) VALUES (?)",
        Some(json!([7])),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let first_row = body["result"][0]["meta"]["last_row_id"]
        .as_i64()
        .unwrap_or(0);
    wait_row(follower.http_addr, token, 7).await;
    wait_row(follower_b.http_addr, token, 7).await;

    let saved = [
        (1_u64, true, None, leader.http_addr, leader.raft_addr),
        (
            2,
            false,
            join.clone(),
            follower.http_addr,
            follower.raft_addr,
        ),
        (3, false, join, follower_b.http_addr, follower_b.raft_addr),
    ];
    leader.close().await;
    follower.close().await;
    follower_b.close().await;

    let (node_a, node_b, node_c) = tokio::join!(
        node_at(
            &root,
            saved[0].0,
            secret,
            token,
            saved[0].1,
            saved[0].2.clone(),
            saved[0].3,
            saved[0].4,
        ),
        node_at(
            &root,
            saved[1].0,
            secret,
            token,
            saved[1].1,
            saved[1].2.clone(),
            saved[1].3,
            saved[1].4,
        ),
        node_at(
            &root,
            saved[2].0,
            secret,
            token,
            saved[2].1,
            saved[2].2.clone(),
            saved[2].3,
            saved[2].4,
        ),
    );
    wait_row(node_a.http_addr, token, 7).await;
    // Node 1 already issued the create and the insert. A reused command id
    // would return the old success and leave 9 unwritten.
    let (status, body) = post(
        node_a.http_addr,
        token,
        "INSERT INTO t(n) VALUES (?)",
        Some(json!([9])),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let new_row = body["result"][0]["meta"]["last_row_id"]
        .as_i64()
        .unwrap_or(0);
    assert!(new_row > first_row, "restart reused a command id: {body}");
    wait_row(node_c.http_addr, token, 9).await;
    let (status, body) = post(node_a.http_addr, token, "SELECT COUNT(*) AS c FROM t", None).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["result"][0]["results"][0]["c"], json!(2), "{body}");
    node_a.close().await;
    node_b.close().await;
    node_c.close().await;
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_write_is_not_cut_by_the_read_timeout() {
    let token = "raft-token";
    let root = std::env::temp_dir().join(format!(
        "osd1-raft-write-timeout-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&root);
    let mut config = NodeConfig::new(1, root.join("1").join("osd1.db"), "cluster-secret");
    config.bootstrap = true;
    config.voter = true;
    config.api_token = Some(token.to_string());
    config.request_timeout = Some(Duration::from_millis(1));
    let running = start_node(config).await.expect("start");
    let (status, body) = post(running.http_addr, token, "CREATE TABLE t(n INTEGER)", None).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["success"], json!(true), "{body}");
    let (status, body) = post(
        running.http_addr,
        token,
        "INSERT INTO t(n) VALUES (?)",
        Some(json!([1])),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["success"], json!(true), "{body}");
    assert!(
        body["result"][0]["meta"]["last_row_id"]
            .as_i64()
            .unwrap_or(0)
            > 0,
        "{body}"
    );
    running.close().await;
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_commit_index_header_waits_on_the_apply_index() {
    let token = "raft-token";
    let root = std::env::temp_dir().join(format!(
        "osd1-raft-commit-index-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&root);
    let mut config = NodeConfig::new(1, root.join("1").join("osd1.db"), "cluster-secret");
    config.bootstrap = true;
    config.voter = true;
    config.api_token = Some(token.to_string());
    config.request_timeout = Some(Duration::from_millis(200));
    let running = start_node(config).await.expect("start");

    let (status, head, body) = post_head(
        running.http_addr,
        token,
        "CREATE TABLE t(n INTEGER); INSERT INTO t(n) VALUES (4)",
        None,
        &[],
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let index = header_value(&head, "x-osd1-commit-index")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    assert!(index >= 1, "missing commit index in {head}");
    let index_header = index.to_string();

    let (status, _head, body) = post_head(
        running.http_addr,
        token,
        "SELECT n FROM t",
        None,
        &[("x-osd1-min-commit-index", &index_header)],
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["result"][0]["results"][0]["n"], json!(4), "{body}");

    let (status, _head, body) = post_head(
        running.http_addr,
        token,
        "SELECT n FROM t",
        None,
        &[("x-osd1-min-commit-index", "18446744073709551615")],
    )
    .await;
    assert_eq!(status, 504, "{body}");
    running.close().await;
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn removed_member_stops_accepting_http() {
    let secret = "cluster-secret";
    let token = "raft-token";
    let root = std::env::temp_dir().join(format!(
        "osd1-raft-remove-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&root);
    let leader = node(&root, 1, secret, token, true, None).await;
    let join = Some(leader.raft_addr.to_string());
    let other = node(&root, 2, secret, token, false, join.clone()).await;
    let target = node(&root, 3, secret, token, false, join).await;
    wait_metrics(leader.http_addr).await;
    osd1_raft::remove_member(&leader.raft_addr.to_string(), secret, target.node_id)
        .await
        .expect("remove");
    let addr = target.http_addr;
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut stopped = false;
    while Instant::now() < deadline {
        let up = tokio::task::spawn_blocking(move || {
            TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok()
        })
        .await
        .expect("connect");
        if !up {
            stopped = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let target_metrics = if stopped {
        String::new()
    } else {
        get_metrics(addr).await
    };
    assert!(
        stopped,
        "removed node kept accepting HTTP on {addr}: {target_metrics}"
    );
    let (status, body) = post(leader.http_addr, token, "SELECT 1", None).await;
    assert_eq!(status, 200, "{body}");
    let removed_id = target.node_id;
    target.close().await;
    let mut again = NodeConfig::new(removed_id, root.join("3").join("osd1.db"), secret);
    again.join = Some(leader.raft_addr.to_string());
    again.voter = true;
    again.api_token = Some(token.to_string());
    match start_node(again).await {
        Ok(running) => {
            running.close().await;
            panic!("a removed database rejoined");
        }
        Err(error) => assert_eq!(error.to_string(), "removed from the raft cluster"),
    }
    leader.close().await;
    other.close().await;
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restored_file_bootstraps_a_new_cluster_and_commits_again() {
    let secret = "cluster-secret";
    let token = "raft-token";
    let root = std::env::temp_dir().join(format!(
        "osd1-raft-restore-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&root);
    let leader = node(&root, 1, secret, token, true, None).await;
    let join = Some(leader.raft_addr.to_string());
    let follower = node(&root, 2, secret, token, false, join.clone()).await;
    let follower_b = node(&root, 3, secret, token, false, join).await;
    let (status, body) = post(leader.http_addr, token, "CREATE TABLE t(n INTEGER)", None).await;
    assert_eq!(status, 200, "{body}");
    let (status, body) = post(
        leader.http_addr,
        token,
        "INSERT INTO t(n) VALUES (?)",
        Some(json!([4])),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    wait_row(follower.http_addr, token, 4).await;
    wait_row(follower_b.http_addr, token, 4).await;
    leader.close().await;
    follower.close().await;
    follower_b.close().await;

    let fresh = root.join("restored");
    copy_database(
        &root.join("1").join("osd1.db"),
        &fresh.join("1").join("osd1.db"),
    );
    assert!(
        !fresh.join("1").join("osd1.raft").exists(),
        "restore copied the raft directory"
    );
    let leader = node(&fresh, 1, secret, token, true, None).await;
    copy_database(
        &fresh.join("1").join("osd1.db"),
        &fresh.join("2").join("osd1.db"),
    );
    copy_database(
        &fresh.join("1").join("osd1.db"),
        &fresh.join("3").join("osd1.db"),
    );
    let join = Some(leader.raft_addr.to_string());
    let follower = node(&fresh, 2, secret, token, false, join.clone()).await;
    let follower_b = node(&fresh, 3, secret, token, false, join).await;
    wait_row(follower_b.http_addr, token, 4).await;
    let (status, body) = post(
        leader.http_addr,
        token,
        "INSERT INTO t(n) VALUES (?)",
        Some(json!([5])),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert!(
        body["result"][0]["meta"]["last_row_id"]
            .as_i64()
            .unwrap_or(0)
            > 0,
        "{body}"
    );
    wait_row(follower_b.http_addr, token, 5).await;
    let (status, body) = post(leader.http_addr, token, "SELECT COUNT(*) AS c FROM t", None).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["result"][0]["results"][0]["c"], json!(2), "{body}");
    leader.close().await;
    follower.close().await;
    follower_b.close().await;
    let _ = std::fs::remove_dir_all(&root);
}

fn copy_database(from: &std::path::Path, to: &std::path::Path) {
    std::fs::create_dir_all(to.parent().expect("parent")).expect("mkdir");
    std::fs::copy(from, to).unwrap_or_else(|error| panic!("copy {from:?}: {error}"));
    for suffix in ["-wal", "-shm"] {
        let mut source = from.as_os_str().to_owned();
        source.push(suffix);
        let source = std::path::PathBuf::from(source);
        if source.is_file() {
            let mut dest = to.as_os_str().to_owned();
            dest.push(suffix);
            std::fs::copy(&source, std::path::PathBuf::from(dest))
                .unwrap_or_else(|error| panic!("copy {source:?}: {error}"));
        }
    }
}

async fn node(
    root: &std::path::Path,
    id: u64,
    secret: &str,
    token: &str,
    bootstrap: bool,
    join: Option<String>,
) -> RunningNode {
    node_at(
        root,
        id,
        secret,
        token,
        bootstrap,
        join,
        "127.0.0.1:0".parse().expect("loopback"),
        "127.0.0.1:0".parse().expect("loopback"),
    )
    .await
}

async fn node_at(
    root: &std::path::Path,
    id: u64,
    secret: &str,
    token: &str,
    bootstrap: bool,
    join: Option<String>,
    http_addr: SocketAddr,
    raft_addr: SocketAddr,
) -> RunningNode {
    let dir = root.join(id.to_string());
    let mut last = String::new();
    for _ in 0..20 {
        let mut config = NodeConfig::new(id, dir.join("osd1.db"), secret);
        config.bootstrap = bootstrap;
        config.join = join.clone();
        config.voter = true;
        config.http_addr = http_addr;
        config.raft_addr = raft_addr;
        config.api_token = Some(token.to_string());
        match start_node(config).await {
            Ok(running) => return running,
            Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {
                last = error.to_string();
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(error) => panic!("start node {id}: {error}"),
        }
    }
    panic!("start node {id}: {last}");
}

async fn wait_row(addr: SocketAddr, token: &str, value: i64) -> Value {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut last = String::new();
    while Instant::now() < deadline {
        let (status, body) = post(
            addr,
            token,
            "SELECT n FROM t WHERE n = ?",
            Some(json!([value])),
        )
        .await;
        last = body.to_string();
        if status == 200 && body["result"][0]["results"][0]["n"] == json!(value) {
            return body;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("row {value} did not appear on {addr}: {last}");
}

async fn wait_metrics(addr: SocketAddr) -> String {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut last = String::new();
    while Instant::now() < deadline {
        last = get_metrics(addr).await;
        if last.contains("osd1_raft_peers 3") && last.contains("osd1_raft_role 1") {
            return last;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    last
}

async fn get_metrics(addr: SocketAddr) -> String {
    let (status, text) = exchange(addr, "GET", "/metrics", None, None).await;
    assert_eq!(status, 200, "{text}");
    text
}

async fn post(addr: SocketAddr, token: &str, sql: &str, params: Option<Value>) -> (u16, Value) {
    let (status, _head, body) = post_head(addr, token, sql, params, &[]).await;
    (status, body)
}

async fn post_head(
    addr: SocketAddr,
    token: &str,
    sql: &str,
    params: Option<Value>,
    extra: &[(&str, &str)],
) -> (u16, String, Value) {
    let body = match params {
        Some(params) => json!({"sql": sql, "params": params}).to_string(),
        None => json!({"sql": sql}).to_string(),
    };
    let (status, head, text) = exchange_head(
        addr,
        "POST",
        "/accounts/a/d1/database/b/query",
        Some(token),
        Some(&body),
        extra,
    )
    .await;
    let parsed = serde_json::from_str(&text).unwrap_or(Value::Null);
    (status, head, parsed)
}

fn header_value(head: &str, name: &str) -> Option<String> {
    head.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.eq_ignore_ascii_case(name)
            .then(|| value.trim().to_string())
    })
}

async fn get(addr: SocketAddr, path: &str, token: &str, sql: &str) -> (u16, String) {
    let body = json!({"sql": sql}).to_string();
    exchange(addr, "POST", path, Some(token), Some(&body)).await
}

async fn exchange(
    addr: SocketAddr,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: Option<&str>,
) -> (u16, String) {
    let (status, _head, text) = exchange_head(addr, method, path, token, body, &[]).await;
    (status, text)
}

async fn exchange_head(
    addr: SocketAddr,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: Option<&str>,
    extra: &[(&str, &str)],
) -> (u16, String, String) {
    let method = method.to_string();
    let path = path.to_string();
    let token = token.map(str::to_string);
    let body = body.map(str::to_string);
    let extra: Vec<(String, String)> = extra
        .iter()
        .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
        .collect();
    tokio::task::spawn_blocking(move || {
        exchange_blocking(
            addr,
            &method,
            &path,
            token.as_deref(),
            body.as_deref(),
            &extra,
        )
    })
    .await
    .expect("http thread")
}

fn exchange_blocking(
    addr: SocketAddr,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: Option<&str>,
    extra: &[(String, String)],
) -> (u16, String, String) {
    let body = body.unwrap_or("");
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    );
    if method == "POST" {
        request.push_str("Content-Type: application/json\r\n");
    }
    if let Some(token) = token {
        request.push_str(&format!("Authorization: Bearer {token}\r\n"));
    }
    for (name, value) in extra {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    request.push_str("\r\n");
    request.push_str(body);
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream.write_all(request.as_bytes()).unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).unwrap();
    let text = String::from_utf8_lossy(&buf).into_owned();
    let (head, resp) = text.split_once("\r\n\r\n").unwrap_or(("", &text));
    let status = head
        .split_whitespace()
        .nth(1)
        .unwrap_or("0")
        .parse()
        .unwrap_or(0);
    (status, head.to_string(), resp.to_string())
}
