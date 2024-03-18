use crate::protocol;
use crate::protocol::{RedisCommand, RedisError, RedisValue};
use anyhow::Context;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::fmt::Write as FmtWrite;
use std::io;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
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

pub struct RedisArguments {
    pub port: u16,
    pub replica_of: Option<String>,
}

#[derive(Clone)]
pub struct RedisServer {
    port: u16,
    master: Option<String>,
    data: Arc<Mutex<HashMap<String, DataEntry>>>,
}

impl RedisServer {
    pub fn new(args: RedisArguments) -> Self {
        let port = args.port;
        let master = args.replica_of;

        Self {
            port,
            master,
            data: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn run(self) -> Result<(), RedisError> {
        let listener = TcpListener::bind(format!("127.0.0.1:{}", self.port))
            .context(format!("failed to listen on {}", self.port))?;

        println!("listening on {}", listener.local_addr().unwrap());

        let server = self.clone();
        let listen_handle = std::thread::spawn(move || server.listen(&listener));

        if self.master.is_some() {
            let slave = self.clone();
            std::thread::spawn(move || slave.init_slave());
        }

        listen_handle.join().unwrap()
    }

    fn listen(&self, listener: &TcpListener) -> Result<(), RedisError> {
        loop {
            match listener.accept() {
                Ok((stream, peer)) => {
                    eprintln!("accepted connection from {}", peer);
                    let mut server = self.clone();
                    std::thread::spawn(move || server.handle_connection(stream));
                }
                Err(e) => {
                    eprintln!("error accepting connection: {}", e);
                    break;
                }
            }
        }

        Ok(())
    }

    fn init_slave(&self) -> Result<(), RedisError> {
        let master = self.master.as_deref().unwrap();

        let stream = TcpStream::connect(master)
            .context(format!("failed to connect to master {}", master))?;

        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut writer = BufWriter::new(stream);

        let commands = vec![
            RedisValue::command("PING", &[]),
            RedisValue::command("REPLCONF", &["listening-port", &self.port.to_string()]),
            RedisValue::command("REPLCONF", &["capa", "psync2"]),
        ];

        for command in commands {
            command
                .write_to(&mut writer)
                .context(format!("failed to send command {} to master", command))?;

            protocol::read_value(&mut reader)?;
        }

        Ok(())
    }

    fn handle_connection(&mut self, stream: TcpStream) -> io::Result<()> {
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut writer = BufWriter::new(stream);

        loop {
            self.read_and_handle_command(&mut reader, &mut writer)?;
        }
    }

    fn read_and_handle_command<R: BufRead, W: Write>(
        &mut self,
        reader: &mut R,
        writer: &mut W,
    ) -> io::Result<()> {
        let command = protocol::read_command(reader);

        let response = match command {
            Ok(command) => self.handle_command(command),
            Err(error) => RedisValue::BulkString(format!("ERR {}", error)),
        };

        response.write_to(writer)
    }

    fn handle_command(&mut self, command: RedisCommand) -> RedisValue {
        match command {
            RedisCommand::Noop => RedisValue::Nil,
            RedisCommand::Ping { message: None } => RedisServer::ping_no_message(),
            RedisCommand::Ping { message: Some(msg) } => RedisServer::ping(msg),
            RedisCommand::Echo { message } => RedisServer::echo(message),
            RedisCommand::Get { key } => self.get(key),
            RedisCommand::Set { key, value, expiry } => self.set(key, value, expiry),
            RedisCommand::Replconf { .. } => RedisServer::replconf(),
            RedisCommand::Info { .. } => self.info(),
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
        RedisValue::ok()
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
        RedisValue::simple_string("PONG")
    }

    fn ping(message: String) -> RedisValue {
        RedisValue::BulkString(message)
    }

    fn echo(message: String) -> RedisValue {
        RedisValue::BulkString(message)
    }

    fn info(&self) -> RedisValue {
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

        RedisValue::BulkString(buffer)
    }

    fn replconf() -> RedisValue {
        RedisValue::ok()
    }
}
