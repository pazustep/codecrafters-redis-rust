mod app;
mod listener;
mod protocol;
mod server;

use std::io;

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    let options = app::parse_options();
    let server = server::start(options.clone());
    listener::start(options, server).await??;
    Ok(())
}
