use protocol::RedisValue;
use server::{RedisCommandHandler, RedisServer};
use std::io;
use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;

mod protocol;
mod server;

#[tokio::main]
async fn main() -> io::Result<()> {
    let args = parse_args();
    let server = RedisServer::new(args.replica_of);
    let listener = TcpListener::bind(format!("127.0.0.1:{}", args.port)).await?;

    println!("listening on {}", listener.local_addr().unwrap());
    let mut tasks = JoinSet::new();

    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                eprintln!("accepted connection from {}", peer);
                let server = server.clone();
                tasks.spawn(handle_connection(stream, server));
            }
            Err(e) => {
                eprintln!("error accepting connection: {}", e);
                break;
            }
        }
    }

    eprintln!("awaiting pending tasks...");
    while tasks.join_next().await.is_some() {}
    eprintln!("done.");

    Ok(())
}

struct Arguments {
    port: u16,
    replica_of: Option<String>,
}

#[derive(Clone, Copy)]
enum ArgState {
    Normal,
    Port,
    ReplicaHost,
    ReplicaPort,
}

fn parse_args() -> Arguments {
    let mut state = ArgState::Normal;
    let mut port: Option<u16> = None;
    let mut replica_host: Option<String> = None;
    let mut replica_port: Option<u16> = None;

    for arg in std::env::args().skip(1) {
        match (state, arg.as_str()) {
            (ArgState::Normal, "--port") => state = ArgState::Port,
            (ArgState::Normal, "--replicaof") => state = ArgState::ReplicaHost,
            (ArgState::Port, value) => {
                port = value.parse().ok();
                state = ArgState::Normal;
            }
            (ArgState::ReplicaHost, value) => {
                replica_host = Some(value.to_string());
                state = ArgState::ReplicaPort;
            }
            (ArgState::ReplicaPort, value) => {
                replica_port = value.parse().ok();
                state = ArgState::Normal;
            }
            (_, value) => {
                eprintln!("ignoring invalid argument: {}", value)
            }
        }
    }

    let replica_of = replica_host.map(|host| {
        let port = replica_port.unwrap_or(DEFAULT_PORT);
        format!("{}:{}", host, port)
    });

    Arguments {
        port: port.unwrap_or(DEFAULT_PORT),
        replica_of,
    }
}

static DEFAULT_PORT: u16 = 6379;

async fn handle_connection(mut stream: TcpStream, mut server: RedisServer) -> io::Result<()> {
    let (read, write) = stream.split();
    let mut reader = BufReader::new(read);
    let mut writer = BufWriter::new(write);

    loop {
        let command = protocol::read_command(&mut reader).await;

        let response = match command {
            Ok(command) => server.handle(command),
            Err(error) => RedisValue::BulkString(format!("ERR {}", error)),
        };

        protocol::write_value(&mut writer, &response).await?;
        writer.flush().await?;
    }
}
