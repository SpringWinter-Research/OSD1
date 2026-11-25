use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::args().nth(1).as_deref() == Some("health") {
        return check_health();
    }
    osd1_server::run().await
}

fn check_health() -> Result<(), Box<dyn std::error::Error>> {
    let raw = std::env::var("OSD1_HTTP_ADDR").unwrap_or_else(|_| "127.0.0.1:8080".to_string());
    let addr: SocketAddr = health_target(&raw).parse()?;
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(2))?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    stream.write_all(b"GET /health HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")?;
    let mut buf = Vec::new();
    let mut chunk = [0u8; 512];
    loop {
        if buf.len() >= 4096 {
            break;
        }
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(err)
                if err.kind() == std::io::ErrorKind::TimedOut
                    || err.kind() == std::io::ErrorKind::WouldBlock =>
            {
                break;
            }
            Err(err) => return Err(err.into()),
        }
    }
    let text = String::from_utf8_lossy(&buf);
    let status_ok = text
        .lines()
        .next()
        .is_some_and(|line| line.contains(" 200 "));
    if status_ok && text.contains("\"ok\":true") {
        Ok(())
    } else {
        Err("health check failed".into())
    }
}

fn health_target(raw: &str) -> String {
    match raw.rsplit_once(':') {
        Some((host, port)) if host == "0.0.0.0" || host == "[::]" || host.is_empty() => {
            format!("127.0.0.1:{port}")
        }
        _ => raw.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::health_target;

    #[test]
    fn health_target_uses_loopback_for_a_wildcard_bind() {
        assert_eq!(health_target("0.0.0.0:8080"), "127.0.0.1:8080");
        assert_eq!(health_target("[::]:8080"), "127.0.0.1:8080");
        assert_eq!(health_target("127.0.0.1:9"), "127.0.0.1:9");
    }
}
