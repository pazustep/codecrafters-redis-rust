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
    let port = get_port_from_args().unwrap_or(DEFAULT_PORT);
    let server = RedisServer::new();

    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).unwrap();
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

static DEFAULT_PORT: u16 = 6379;

fn get_port_from_args() -> Option<u16> {
    let args = std::env::args().collect::<Vec<_>>();

    if args.len() > 2 && args[1] == "--port" {
        args[2].parse::<u16>().ok()
    } else {
        None
    }
}

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
