use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

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
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }

    let worker = SqliteWorker::open(&path, capacity, served_by_colo)?;
    let executor: Arc<dyn SqlExecutor> = Arc::new(worker);
    let app = router(AppState {
        executor,
        api_token,
    });

    let listener = tokio::net::TcpListener::bind(addr).await?;
    eprintln!("osd1 listening on {addr} database {db_path}");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
