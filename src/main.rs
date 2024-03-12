mod protocol;

use protocol::{RedisCommand, RedisValue};
use std::{
    collections::HashMap,
    io::BufReader,
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
};

fn main() {
    let server = ServerState::new();
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

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

fn handle_connection(mut stream: TcpStream, mut server: ServerState) -> std::io::Result<()> {
    loop {
        let command = {
            let mut reader = BufReader::new(&stream);
            protocol::read_command(&mut reader)
        };

        let response = match command {
            Ok(command) => command.process(&mut server),
            Err(error) => RedisValue::BulkString(format!("ERR {}", error)),
        };

        protocol::write_value(&mut stream, &response)?;
    }
}

#[derive(Clone)]
struct ServerState {
    data: Arc<Mutex<HashMap<String, String>>>,
}

impl ServerState {
    pub fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn set(&mut self, key: String, value: String) {
        let mut data = self.data.lock().unwrap();
        data.insert(key, value);
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let data = self.data.lock().unwrap();
        data.get(key).cloned()
    }
}

trait CommandProcessor {
    fn process(self, server: &mut ServerState) -> RedisValue;
}

fn ping_no_message() -> RedisValue {
    RedisValue::SimpleString("PONG".to_string())
}

fn ping(message: String) -> RedisValue {
    RedisValue::BulkString(message)
}

fn echo(message: String) -> RedisValue {
    RedisValue::BulkString(message)
}

fn get(server: &ServerState, key: String) -> RedisValue {
    server
        .get(&key)
        .map_or(RedisValue::Nil, RedisValue::BulkString)
}

fn set(server: &mut ServerState, key: String, value: String) -> RedisValue {
    server.set(key, value);
    RedisValue::SimpleString("OK".to_string())
}

impl CommandProcessor for RedisCommand {
    fn process(self, server: &mut ServerState) -> RedisValue {
        match self {
            RedisCommand::Ping { message: None } => ping_no_message(),
            RedisCommand::Ping { message: Some(msg) } => ping(msg),
            RedisCommand::Echo { message } => echo(message),
            RedisCommand::Get { key } => get(server, key),
            RedisCommand::Set { key, value } => set(server, key, value),
        }
    }
}
