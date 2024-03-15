use crate::protocol::{RedisCommand, RedisValue};
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::fmt::Write;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub trait RedisCommandHandler {
    fn handle(&mut self, command: RedisCommand) -> RedisValue;
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

#[derive(Clone)]
pub struct RedisServer {
    info: HashMap<String, HashMap<String, String>>,
    data: Arc<Mutex<HashMap<String, DataEntry>>>,
}

impl RedisServer {
    pub fn new(master: Option<String>) -> Self {
        let role = match master {
            Some(_) => "slave",
            None => "master",
        };

        let mut replication = HashMap::new();
        replication.insert("role".to_string(), role.to_string());

        let mut info = HashMap::new();
        info.insert("Replication".to_string(), replication);

        Self {
            info,
            data: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn set(&mut self, key: String, value: String, expiry: Option<u64>) -> RedisValue {
        let mut data = self.data.lock().unwrap();
        let entry = DataEntry {
            value,
            expiry: expiry.map(Duration::from_millis),
            last_accessed: Instant::now(),
        };

        data.insert(key, entry);
        RedisValue::SimpleString("OK".to_string())
    }

    fn get(&mut self, key: String) -> RedisValue {
        let mut data = self.data.lock().unwrap();

        let value = match data.entry(key) {
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
        };

        match value {
            None => RedisValue::Nil,
            Some(value) => RedisValue::BulkString(value),
        }
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

    fn info(&self) -> RedisValue {
        let mut buffer = String::new();
        let buffer_ref = &mut buffer;

        for (section_title, section_info) in &self.info {
            writeln!(buffer_ref, "# {}", section_title).unwrap();

            for (key, value) in section_info {
                writeln!(buffer_ref, "{}:{}", key, value).unwrap();
            }
        }

        RedisValue::BulkString(buffer)
    }
}

impl RedisCommandHandler for RedisServer {
    fn handle(&mut self, command: RedisCommand) -> RedisValue {
        match command {
            RedisCommand::Noop => RedisValue::Nil,
            RedisCommand::Ping { message: None } => RedisServer::ping_no_message(),
            RedisCommand::Ping { message: Some(msg) } => RedisServer::ping(msg),
            RedisCommand::Echo { message } => RedisServer::echo(message),
            RedisCommand::Get { key } => self.get(key),
            RedisCommand::Set { key, value, expiry } => self.set(key, value, expiry),
            RedisCommand::Info { .. } => self.info(),
        }
    }
}
