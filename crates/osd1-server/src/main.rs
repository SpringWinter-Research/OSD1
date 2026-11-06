use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use osd1_engine::SqliteWorker;
use osd1_http::{router, AppState};
use osd1_types::SqlExecutor;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = env::var("OSD1_DB_PATH").unwrap_or_else(|_| "./data/osd1.db".to_string());
    let addr: SocketAddr = env::var("OSD1_HTTP_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8080".to_string())
        .parse()?;
    let api_token = env::var("OSD1_API_TOKEN")
        .ok()
        .filter(|token| !token.is_empty());
    let capacity = env::var("OSD1_QUEUE_CAPACITY")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(64);
    let served_by_colo = env::var("OSD1_SERVED_BY_COLO").unwrap_or_else(|_| "LOC".to_string());

    let path = PathBuf::from(&db_path);
    let worker = SqliteWorker::open(&path, capacity, served_by_colo)?;
    let executor: Arc<dyn SqlExecutor> = Arc::new(worker);
    let mut state = AppState::new(executor, api_token.clone());
    state.max_body_bytes = env_usize("OSD1_MAX_BODY_BYTES", state.max_body_bytes);
    state.max_response_bytes = env_usize("OSD1_MAX_RESPONSE_BYTES", state.max_response_bytes);
    state.request_timeout = match env_u64("OSD1_REQUEST_TIMEOUT_MS", 30_000) {
        0 => None,
        millis => Some(Duration::from_millis(millis)),
    };
    let app = router(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    eprintln!("{}", startup_line(&addr, &db_path, api_token.as_deref()));
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn startup_line(addr: &SocketAddr, db_path: &str, token: Option<&str>) -> String {
    redact(
        &format!("osd1 listening on {addr} database {db_path}"),
        token,
    )
}

/// Replace the configured bearer token if it would otherwise be written to a log line.
fn redact(text: &str, secret: Option<&str>) -> String {
    match secret {
        Some(secret) if !secret.is_empty() => text.replace(secret, "[redacted]"),
        _ => text.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn startup_line_redacts_the_api_token() {
        let line = startup_line(
            &"127.0.0.1:8080".parse().unwrap(),
            "/data/topsecret-token/osd1.db",
            Some("topsecret-token"),
        );
        assert!(!line.contains("topsecret-token"));
        assert!(line.contains("[redacted]"));
        assert!(line.contains("osd1 listening"));
    }
}
