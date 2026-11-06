#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    osd1_server::run().await
}
