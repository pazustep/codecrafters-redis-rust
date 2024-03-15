use protocol::RedisValue;
use server::{RedisCommandHandler, RedisServer};
use std::io::{BufWriter, Write};
use std::{
    io::BufReader,
    net::{TcpListener, TcpStream},
};

mod protocol;
mod server;

fn main() {
    let args = parse_args();
    let server = RedisServer::new(args.replica_of);

    let listener = TcpListener::bind(format!("127.0.0.1:{}", args.port)).unwrap();
    println!("listening on {}", listener.local_addr().unwrap());

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let server = server.clone();
                std::thread::spawn(move || handle_connection(stream, server));
            }
            Err(e) => {
                eprintln!("error: {}", e);
            }
        }
    }
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

fn handle_connection(stream: TcpStream, mut server: RedisServer) -> std::io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = BufWriter::new(stream);

    loop {
        let command = protocol::read_command(&mut reader);

        let response = match command {
            Ok(command) => server.handle(command),
            Err(error) => RedisValue::BulkString(format!("ERR {}", error)),
        };

        protocol::write_value(&mut writer, &response)?;
        writer.flush()?;
    }
}
