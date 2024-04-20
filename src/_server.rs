use crate::protocol;
use crate::protocol::{RedisCommand, RedisError, RedisValue};
use anyhow::Context;
use std::collections::{
    hash_map::Entry::{Occupied, Vacant},
    VecDeque,
};
use std::io::{BufReader, BufWriter};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{collections::HashMap, ops::Deref};
use std::{fmt::Write as FmtWrite, ops::DerefMut};
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
    replica_state: Arc<Mutex<ReplicaState>>,
}

enum ReplicaState {
    Syncing { buffer: Vec<RedisCommand> },
    Initialized,
}

impl RedisServer {
    pub fn new(args: RedisArguments) -> Self {
        let port = args.port;
        let master = args.replica_of;
        let replica_state = match master {
            Some(_) => ReplicaState::Syncing { buffer: vec![] },
            None => ReplicaState::Initialized,
        };

        Self {
            port,
            master,
            data: Arc::new(Mutex::new(HashMap::new())),
            replicas: Arc::new(Mutex::new(vec![])),
            replica_state: Arc::new(Mutex::new(replica_state)),
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
            std::thread::spawn(move || {
                let result = replica.run_replica();
                match result {
                    Ok(()) => {
                        println!("replica connection closed");
                    }
                    Err(err) => {
                        eprintln!("replica connection closed with error: {}", err)
                    }
                }
            });
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
                    std::thread::spawn(move || {
                        let result = server.handle_connection(stream, true);
                        match result {
                            Ok(_) => {
                                eprintln!("connection from {} closed", peer);
                            }
                            Err(err) => {
                                eprintln!("connection from {} closed with error: {}", peer, err)
                            }
                        }
                    });
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
            RedisValue::command("PING", &[]),
            RedisValue::command("REPLCONF", &["listening-port", &self.port.to_string()]),
            RedisValue::command("REPLCONF", &["capa", "psync2"]),
            RedisValue::command("PSYNC", &["?", "-1"]),
        ];

        for command in commands {
            command
                .write_to(&mut writer)
                .context(format!("failed to send command {} to master", command))?;

            let reply = protocol::read_value(&mut reader)?;
            println!("received reply to replica command {}: {}", &command, reply);
        }

        println!("waiting for RDB file...");

        match protocol::read_bulk_bytes(&mut reader)? {
            RedisValue::BulkBytes(rdb) => {
                println!("received RDB file with {} bytes", rdb.len());
            }
            _ => {
                println!("received unexpected redis value");
            }
        }

        {
            println!("acquiring replica_state lock to replay buffered commands");
            let mut replica_state = self.replica_state.lock().unwrap();

            if let ReplicaState::Syncing { buffer } = replica_state.deref() {
                println!("replaying {} commands", buffer.len());

                for command in buffer {
                    self.apply_command(&stream, command);
                }
            }

            *replica_state = ReplicaState::Initialized;
        }

        println!("replica initialized");

        // process commands received from the master
        let result = self
            .handle_connection(stream, false)
            .context("I/O error handling replica connection");

        match result {
            Ok(()) => {
                println!("closing replica connection");
            }
            Err(err) => {
                eprintln!("replica connection closed with error {}", err);
            }
        }

        Ok(())
    }

    fn handle_connection(&self, stream: TcpStream, send_replies: bool) -> io::Result<()> {
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut writer = BufWriter::new(stream.try_clone().unwrap());

        loop {
            match protocol::read_command(&mut reader) {
                Ok(ref command) => {
                    println!("handling command {}", command.to_value());
                    self.handle_command(&stream, &mut writer, command, send_replies)?;
                }
                Err(error) => {
                    let reply = RedisValue::BulkString(format!("ERR {}", error));
                    reply.write_to(&mut writer)?;
                }
            }
        }
    }

    fn handle_command(
        &self,
        stream: &TcpStream,
        writer: &mut BufWriter<TcpStream>,
        command: &RedisCommand,
        send_replies: bool,
    ) -> io::Result<()> {
        if !command.is_replica() {
            println!("acquiring replica_state lock to handle command");
            let mut replica_state = self.replica_state.lock().unwrap();

            if let ReplicaState::Syncing { buffer } = replica_state.deref_mut() {
                println!("buffering command {}", command.to_value());
                buffer.push(command.clone());
                return Ok(());
            }
        }

        let response = self.apply_command(stream, command);

        if send_replies {
            for value in response {
                println!(
                    "handled command {}; sending reply {}",
                    command.to_value(),
                    value
                );
                value.write_to(writer)?;
            }
        } else {
            println!(
                "handled command {}; not sending a reply",
                command.to_value()
            );
        }

        if !self.is_replica() && command.is_write() {
            self.replicate_command(command);
        }

        Ok(())
    }

    fn replicate_command(&self, command: &RedisCommand) {
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

    fn apply_command(&self, stream: &TcpStream, command: &RedisCommand) -> Vec<RedisValue> {
        match command {
            RedisCommand::Noop => vec![RedisValue::NullBulkString],
            RedisCommand::Ping { message: None } => RedisServer::ping_no_message(),
            RedisCommand::Ping { message: Some(msg) } => RedisServer::ping(msg),
            RedisCommand::Echo { message } => RedisServer::echo(message),
            RedisCommand::Get { key } => self.get(key),
            RedisCommand::Set { key, value, expiry } => self.set(key, value, expiry.to_owned()),
            RedisCommand::Replconf { key, value } => RedisServer::replconf(key, value),
            RedisCommand::Psync { .. } => self.psync(stream),
            RedisCommand::Info { .. } => self.info(),
        }
    }

    fn set(&self, key: &str, value: &str, expiry: Option<u64>) -> Vec<RedisValue> {
        let mut data = self.data.lock().unwrap();
        let entry = DataEntry {
            value: value.to_string(),
            expiry: expiry.map(Duration::from_millis),
            last_accessed: Instant::now(),
        };

        data.insert(key.to_string(), entry);
        vec![RedisValue::ok()]
    }

    fn get(&self, key: &str) -> Vec<RedisValue> {
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
            None => vec![RedisValue::NullBulkString],
            Some(value) => vec![RedisValue::BulkString(value)],
        }
    }

    fn ping_no_message() -> Vec<RedisValue> {
        vec![RedisValue::simple_string("PONG")]
    }

    fn ping(message: &str) -> Vec<RedisValue> {
        vec![RedisValue::bulk_string(message)]
    }

    fn echo(message: &str) -> Vec<RedisValue> {
        vec![RedisValue::bulk_string(message)]
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

    fn replconf(key: &str, _value: &str) -> Vec<RedisValue> {
        match key.to_lowercase().as_str() {
            "getack" => vec![RedisValue::command("REPLCONF", &["ACK", "0"])],
            _ => vec![RedisValue::ok()],
        }
    }

    fn psync(&self, stream: &TcpStream) -> Vec<RedisValue> {
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

        vec![
            RedisValue::simple_string(response),
            RedisValue::BulkBytes(rdb_bytes),
        ]
    }
}
