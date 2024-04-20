mod app;
mod protocol;
mod server;

use protocol::RedisError;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
    let options = app::parse_options();
    server::RedisServer::start(options).await
}
