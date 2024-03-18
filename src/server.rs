use crate::protocol;
use crate::protocol::{RedisCommand, RedisError, RedisValue};
use anyhow::Context;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::fmt::Write as FmtWrite;
use std::io::{BufRead, BufReader, BufWriter, Write};
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
            RedisValue::command("PSYNC", &["?", "-1"]),
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
            Err(error) => vec![RedisValue::BulkString(format!("ERR {}", error))],
        };

        for value in response {
            value.write_to(writer)?;
        }

        Ok(())
    }

    fn handle_command(&mut self, command: RedisCommand) -> Vec<RedisValue> {
        match command {
            RedisCommand::Noop => vec![RedisValue::Nil],
            RedisCommand::Ping { message: None } => RedisServer::ping_no_message(),
            RedisCommand::Ping { message: Some(msg) } => RedisServer::ping(msg),
            RedisCommand::Echo { message } => RedisServer::echo(message),
            RedisCommand::Get { key } => self.get(key),
            RedisCommand::Set { key, value, expiry } => self.set(key, value, expiry),
            RedisCommand::Replconf { .. } => RedisServer::replconf(),
            RedisCommand::Psync { .. } => RedisServer::psync(),
            RedisCommand::Info { .. } => self.info(),
        }
    }

    fn set(&mut self, key: String, value: String, expiry: Option<u64>) -> Vec<RedisValue> {
        let mut data = self.data.lock().unwrap();
        let entry = DataEntry {
            value,
            expiry: expiry.map(Duration::from_millis),
            last_accessed: Instant::now(),
        };

        data.insert(key, entry);
        vec![RedisValue::ok()]
    }

    fn get(&mut self, key: String) -> Vec<RedisValue> {
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
            None => vec![RedisValue::Nil],
            Some(value) => vec![RedisValue::BulkString(value)],
        }
    }

    fn ping_no_message() -> Vec<RedisValue> {
        vec![RedisValue::simple_string("PONG")]
    }

    fn ping(message: String) -> Vec<RedisValue> {
        vec![RedisValue::BulkString(message)]
    }

    fn echo(message: String) -> Vec<RedisValue> {
        vec![RedisValue::BulkString(message)]
    }

    fn info(&self) -> Vec<RedisValue> {
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

        vec![RedisValue::BulkString(buffer)]
    }

    fn replconf() -> Vec<RedisValue> {
        vec![RedisValue::ok()]
    }

    fn psync() -> Vec<RedisValue> {
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

        vec![
            RedisValue::simple_string(response),
            RedisValue::BulkBytes(rdb_bytes),
        ]
    }
}
