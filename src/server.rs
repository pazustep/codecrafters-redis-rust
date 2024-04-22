use crate::protocol;
use crate::protocol::{Command, RedisError, Value};
use anyhow::Context;
use std::collections::HashMap;
use std::collections::{
    hash_map::Entry::{Occupied, Vacant},
    VecDeque,
};
use std::fmt::Write as FmtWrite;
use std::io::{BufReader, BufWriter};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{io, vec};

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

pub struct RedisArguments {
    pub port: u16,
    pub replica_of: Option<String>,
}

#[derive(Clone)]
pub struct RedisServer {
    port: u16,
    master: Option<String>,
    data: Arc<Mutex<HashMap<String, DataEntry>>>,
    replicas: Arc<Mutex<Vec<TcpStream>>>,
}

impl RedisServer {
    pub fn new(args: RedisArguments) -> Self {
        let port = args.port;
        let master = args.replica_of;

        Self {
            port,
            master,
            data: Arc::new(Mutex::new(HashMap::new())),
            replicas: Arc::new(Mutex::new(vec![])),
        }
    }

    pub fn run(self) -> Result<(), RedisError> {
        let listener = TcpListener::bind(format!("127.0.0.1:{}", self.port))
            .context(format!("failed to listen on {}", self.port))?;

        println!("listening on {}", listener.local_addr().unwrap());

        let server = self.clone();
        let listen_handle = std::thread::spawn(move || server.listen(&listener));

        if self.is_replica() {
            let replica = self.clone();
            std::thread::spawn(move || replica.run_replica());
        }

        listen_handle.join().unwrap()
    }

    fn is_replica(&self) -> bool {
        self.master.is_some()
    }

    fn listen(&self, listener: &TcpListener) -> Result<(), RedisError> {
        loop {
            match listener.accept() {
                Ok((stream, peer)) => {
                    eprintln!("accepted connection from {}", peer);
                    let server = self.clone();
                    std::thread::spawn(move || server.handle_connection(stream, true));
                }
                Err(e) => {
                    eprintln!("error accepting connection: {}", e);
                    break;
                }
            }
        }

        Ok(())
    }

    fn run_replica(&self) -> Result<(), RedisError> {
        let master = self.master.as_deref().unwrap();

        let stream = TcpStream::connect(master)
            .context(format!("failed to connect to master {}", master))?;

        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut writer = BufWriter::new(stream.try_clone().unwrap());

        let commands = vec![
            Value::command("PING", &[]),
            Value::command("REPLCONF", &["listening-port", &self.port.to_string()]),
            Value::command("REPLCONF", &["capa", "psync2"]),
            Value::command("PSYNC", &["?", "-1"]),
        ];

        for command in commands {
            command
                .write_to(&mut writer)
                .context(format!("failed to send command {} to master", command))?;

            protocol::read_value(&mut reader)?;
        }

        // process commands received from the master
        self.handle_connection(stream, false)
            .context("I/O error handling replica connection")?;

        Ok(())
    }

    fn handle_connection(&self, stream: TcpStream, send_replies: bool) -> io::Result<()> {
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut writer = BufWriter::new(stream.try_clone().unwrap());

        loop {
            let command = protocol::read_command(&mut reader);

            let response = match &command {
                Ok(command) => self.handle_command(&stream, command),
                Err(error) => vec![Value::BulkString(format!("ERR {}", error))],
            };

            if send_replies {
                for value in response {
                    value.write_to(&mut writer)?;
                }
            }

            if let Ok(command) = command {
                if !self.is_replica() && command.is_write() {
                    let mut replicas = self.replicas.lock().unwrap();
                    let mut failed = VecDeque::with_capacity(replicas.len());

                    for (index, replica) in replicas.iter_mut().enumerate() {
                        let value = command.to_value();
                        let mut writer = BufWriter::new(replica.try_clone().unwrap());

                        if value.write_to(&mut writer).is_err() {
                            eprintln!(
                                "failed to write to replica {}; will drop",
                                replica.peer_addr().unwrap()
                            );
                            failed.push_front(index)
                        }
                    }

                    for index in failed {
                        replicas.remove(index);
                    }
                }
            }
        }
    }

    fn handle_command(&self, stream: &TcpStream, command: &Command) -> Vec<Value> {
        match command {
            Command::Noop => vec![Value::Nil],
            Command::Ping { message: None } => RedisServer::ping_no_message(),
            Command::Ping { message: Some(msg) } => RedisServer::ping(msg),
            Command::Echo { message } => RedisServer::echo(message),
            Command::Get { key } => self.get(key),
            Command::Set { key, value, expiry } => self.set(key, value, expiry.to_owned()),
            Command::Replconf { .. } => RedisServer::replconf(),
            Command::Psync { .. } => self.psync(stream),
            Command::Info { .. } => self.info(),
        }
    }

    fn set(&self, key: &str, value: &str, expiry: Option<u64>) -> Vec<Value> {
        let mut data = self.data.lock().unwrap();
        let entry = DataEntry {
            value: value.to_string(),
            expiry: expiry.map(Duration::from_millis),
            last_accessed: Instant::now(),
        };

        data.insert(key.to_string(), entry);
        vec![Value::ok()]
    }

    fn get(&self, key: &str) -> Vec<Value> {
        let mut data = self.data.lock().unwrap();

        let value = match data.entry(key.to_string()) {
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
            None => vec![Value::Nil],
            Some(value) => vec![Value::BulkString(value)],
        }
    }

    fn ping_no_message() -> Vec<Value> {
        vec![Value::simple_string("PONG")]
    }

    fn ping(message: &str) -> Vec<Value> {
        vec![Value::bulk_string(message)]
    }

    fn echo(message: &str) -> Vec<Value> {
        vec![Value::bulk_string(message)]
    }

    fn info(&self) -> Vec<Value> {
        let role = match self.master.as_deref() {
            Some(_) => "slave",
            None => "master",
        };

        let mut buffer = String::new();
        let buffer_ref = &mut buffer;

        writeln!(buffer_ref, "# Replication").unwrap();
        writeln!(buffer_ref, "role:{}", role).unwrap();
        writeln!(
            buffer_ref,
            "master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        )
        .unwrap();
        writeln!(buffer_ref, "master_repl_offset:0").unwrap();

        vec![Value::BulkString(buffer)]
    }

    fn replconf() -> Vec<Value> {
        vec![Value::ok()]
    }

    fn psync(&self, stream: &TcpStream) -> Vec<Value> {
        let response = "FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0";

        let rdb_hex = concat!(
            "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473",
            "c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d626173",
            "65c000fff06e3bfec0ff5aa2"
        );

        let mut rdb_bytes = Vec::with_capacity(rdb_hex.len() / 2);
        let mut parse_buffer = String::with_capacity(2);

        let chars = rdb_hex.chars().collect::<Vec<_>>();
        for chunk in chars.chunks_exact(2) {
            write!(&mut parse_buffer, "{}{}", chunk[0], chunk[1]).unwrap();

            let value = u8::from_str_radix(&parse_buffer, 16)
                .context(format!("failed to parse buffer {}", parse_buffer))
                .unwrap();

            rdb_bytes.push(value);
            parse_buffer.clear();
        }

        let mut replicas = self.replicas.lock().unwrap();
        replicas.push(stream.try_clone().unwrap());

        vec![Value::simple_string(response), Value::BulkBytes(rdb_bytes)]
    }
}
