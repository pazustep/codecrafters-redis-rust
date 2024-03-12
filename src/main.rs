mod protocol;

use protocol::{RedisCommand, RedisValue};
use std::{
    collections::{
        hash_map::Entry::{Occupied, Vacant},
        HashMap,
    },
    io::BufReader,
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
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
    data: Arc<Mutex<HashMap<String, DataEntry>>>,
}

struct DataEntry {
    value: String,
    expiry: Option<Duration>,
    last_accessed: Instant,
}

impl DataEntry {
    fn is_expired(&self) -> bool {
        match self.expiry {
            None => false,
            Some(expiry) => {
                let duration = Instant::now().duration_since(self.last_accessed);
                duration > expiry
            }
        }
    }
}

impl ServerState {
    pub fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn set(&mut self, key: String, value: String, expiry: Option<u64>) {
        let mut data = self.data.lock().unwrap();
        let entry = DataEntry {
            value,
            expiry: expiry.map(Duration::from_millis),
            last_accessed: Instant::now(),
        };

        data.insert(key, entry);
    }

    pub fn get(&mut self, key: String) -> Option<String> {
        let mut data = self.data.lock().unwrap();

        match data.entry(key) {
            Vacant(_) => None,
            Occupied(entry) if entry.get().is_expired() => {
                entry.remove();
                None
            }
            Occupied(mut entry) => {
                let entry = entry.get_mut();
                entry.last_accessed = Instant::now();
                Some(entry.value.clone())
            }
        }
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

fn get(server: &mut ServerState, key: String) -> RedisValue {
    server
        .get(key)
        .map_or(RedisValue::Nil, RedisValue::BulkString)
}

fn set(server: &mut ServerState, key: String, value: String, expiry: Option<u64>) -> RedisValue {
    server.set(key, value, expiry);
    RedisValue::SimpleString("OK".to_string())
}

impl CommandProcessor for RedisCommand {
    fn process(self, server: &mut ServerState) -> RedisValue {
        match self {
            RedisCommand::Ping { message: None } => ping_no_message(),
            RedisCommand::Ping { message: Some(msg) } => ping(msg),
            RedisCommand::Echo { message } => echo(message),
            RedisCommand::Get { key } => get(server, key),
            RedisCommand::Set { key, value, expiry } => set(server, key, value, expiry),
        }
    }
}
