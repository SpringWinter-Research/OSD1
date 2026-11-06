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
    assert!(metrics.contains("osd1_raft_peers 3"), "{metrics}");
    assert!(!metrics.contains(secret));
    assert!(!metrics.contains("SELECT"));
    let follower_metrics = get_metrics(follower.http_addr).await;
    assert!(
        follower_metrics.contains("osd1_raft_role 0"),
        "{follower_metrics}"
    );

    let refused =
        osd1_raft::join_cluster(&leader.raft_addr.to_string(), "nope", 9, "127.0.0.1:1").await;
    assert!(refused.is_err(), "{refused:?}");

    leader.close().await;
    follower.close().await;
    follower_b.close().await;
    let _ = std::fs::remove_dir_all(&root);
}

async fn node(
    root: &std::path::Path,
    id: u64,
    secret: &str,
    token: &str,
    bootstrap: bool,
    join: Option<String>,
) -> RunningNode {
    let dir = root.join(id.to_string());
    let mut config = NodeConfig::new(id, dir.join("osd1.db"), secret);
    config.bootstrap = bootstrap;
    config.join = join;
    config.api_token = Some(token.to_string());
    start_node(config).await.expect("start node")
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
    let body = match params {
        Some(params) => json!({"sql": sql, "params": params}).to_string(),
        None => json!({"sql": sql}).to_string(),
    };
    let (status, text) = exchange(
        addr,
        "POST",
        "/accounts/a/d1/database/b/query",
        Some(token),
        Some(&body),
    )
    .await;
    let parsed = serde_json::from_str(&text).unwrap_or(Value::Null);
    (status, parsed)
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
    let method = method.to_string();
    let path = path.to_string();
    let token = token.map(str::to_string);
    let body = body.map(str::to_string);
    tokio::task::spawn_blocking(move || {
        exchange_blocking(addr, &method, &path, token.as_deref(), body.as_deref())
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
) -> (u16, String) {
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
    (status, resp.to_string())
}
