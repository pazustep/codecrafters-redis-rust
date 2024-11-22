mod database;
mod replication;

use crate::{
    protocol::{Command, Value},
    server::replication::ReplicationManager,
};
use database::Database;
use std::{collections::HashMap, net::SocketAddr, time::Duration};
use tokio::sync::{mpsc, oneshot};

use self::replication::ReplicationError;

#[derive(Clone)]
pub struct ServerOptions {
    pub port: u16,
    pub replica_of: Option<String>,
}

#[derive(Clone)]
pub struct ServerHandle {
    sender: mpsc::UnboundedSender<ServerMessage>,
}

#[derive(Debug, thiserror::Error)]
#[error("failed to send command to server")]
pub struct ServerSendError;

impl ServerHandle {
    pub fn send(
        &self,
        command: Command,
        reply_to: mpsc::UnboundedSender<Vec<Value>>,
    ) -> Result<(), ServerSendError> {
        let envelope = ServerMessage::ProcessCommand { command, reply_to };
        self.sender.send(envelope).map_err(|_| ServerSendError)
    }

    pub fn add_replica(
        &self,
        address: SocketAddr,
        values_sender: mpsc::UnboundedSender<Vec<Value>>,
    ) -> Result<(), ServerSendError> {
        let message = ServerMessage::AddReplica {
            address,
            values_sender,
        };
        self.sender.send(message).map_err(|_| ServerSendError)
    }
}

enum ServerMessage {
    ProcessCommand {
        command: Command,
        reply_to: mpsc::UnboundedSender<Vec<Value>>,
    },
    AddReplica {
        address: SocketAddr,
        values_sender: mpsc::UnboundedSender<Vec<Value>>,
    },
}

pub fn start(options: ServerOptions) -> ServerHandle {
    let (tx, rx) = mpsc::unbounded_channel::<ServerMessage>();
    let server = ServerHandle { sender: tx };

    let repl_init = replication::start(options.clone(), server.clone());
    tokio::spawn(async move { command_loop(options, repl_init, rx).await });

    server
}

async fn command_loop(
    options: ServerOptions,
    repl_init: oneshot::Receiver<Result<(), ReplicationError>>,
    mut receiver: mpsc::UnboundedReceiver<ServerMessage>,
) {
    let mut server = Server::new(options);

    match repl_init.await.unwrap() {
        Ok(()) => {
            println!("replica initialization completed successfully; now processing commands")
        }
        Err(err) => {
            println!("failed to start replication: {}", err);
            return;
        }
    }

    loop {
        match receiver.recv().await {
            None => {
                println!("server channel closed; exiting task");
                break;
            }
            Some(ServerMessage::ProcessCommand { command, reply_to }) => {
                let response = server.handle(command.clone());

                if reply_to.send(response).is_err() {
                    println!("failed to send response to client; ignoring");
                } else {
                    server.replication.replicate(&command);
                }
            }
            Some(ServerMessage::AddReplica {
                address,
                values_sender,
            }) => {
                server.replication.add(address, values_sender);
            }
        }
    }
}

struct Server {
    options: ServerOptions,
    database: Database,
    replication: ReplicationManager,
    offset: usize,
}

impl Server {
    pub fn new(options: ServerOptions) -> Self {
        Self {
            options,
            database: Database::new(),
            replication: ReplicationManager::new(),
            offset: 0,
        }
    }

    fn handle(&mut self, command: Command) -> Vec<Value> {
        let (size, response) = match command {
            Command::Ping { size, message } => (size, self.ping(message)),
            Command::Echo { size, message } => (size, self.echo(message)),
            Command::Get { size, key, .. } => (size, self.get(key)),
            Command::Set {
                size,
                key,
                value,
                expiry,
                ..
            } => (size, self.set(key, value, expiry)),
            Command::Info { size, .. } => (size, self.info()),
            Command::Replconf {
                size, key, value, ..
            } => (size, self.replconf(&key, &value)),
            Command::Psync { size, .. } => (size, self.psync()),
        };

        self.offset += size;
        response
    }

    fn ping(&self, message: Option<Vec<u8>>) -> Vec<Value> {
        let response = match message {
            None => Value::simple_string("PONG"),
            Some(message) => Value::bulk_string_from_bytes(message),
        };

        vec![response]
    }

    fn echo(&self, message: Vec<u8>) -> Vec<Value> {
        vec![Value::bulk_string_from_bytes(message)]
    }

    fn get(&mut self, key: Vec<u8>) -> Vec<Value> {
        match self.database.get(key) {
            Some(value) => vec![Value::bulk_string_from_bytes(value)],
            None => vec![Value::NullBulkString],
        }
    }

    fn set(&mut self, key: Vec<u8>, value: Vec<u8>, expiry: Option<Duration>) -> Vec<Value> {
        self.database.set(key, value, expiry);
        vec![Value::ok()]
    }

    fn info(&self) -> Vec<Value> {
        let mut info = HashMap::new();

        if self.options.replica_of.is_some() {
            info.insert("role".to_string(), "slave".to_string());
        } else {
            info.insert("role".to_string(), "master".to_string());
            info.insert(
                "master_replid".to_string(),
                "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
            );
            info.insert("master_repl_offset".to_string(), "0".to_string());
        }

        let result = info
            .iter()
            .map(|(key, value)| format!("{}:{}", key, value))
            .collect::<Vec<_>>()
            .join("\r\n");

        vec![Value::bulk_string_from_bytes(result.into_bytes())]
    }

    fn replconf(&self, key: &[u8], _value: &[u8]) -> Vec<Value> {
        if String::from_utf8_lossy(key).to_uppercase() == "GETACK" {
            let offset = format!("{}", self.offset);
            vec![Value::command_str("REPLCONF", &["ACK", &offset])]
        } else {
            vec![Value::ok()]
        }
    }

    fn psync(&self) -> Vec<Value> {
        let rdb = vec![
            82, 69, 68, 73, 83, 48, 48, 49, 49, 250, 9, 114, 101, 100, 105, 115, 45, 118, 101, 114,
            5, 55, 46, 50, 46, 48, 250, 10, 114, 101, 100, 105, 115, 45, 98, 105, 116, 115, 192,
            64, 250, 5, 99, 116, 105, 109, 101, 194, 109, 8, 188, 101, 250, 8, 117, 115, 101, 100,
            45, 109, 101, 109, 194, 176, 196, 16, 0, 250, 8, 97, 111, 102, 45, 98, 97, 115, 101,
            192, 0, 255, 240, 110, 59, 254, 192, 255, 90, 162,
        ];

        vec![
            Value::simple_string("FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0"),
            Value::BulkBytes((rdb.len(), rdb)),
        ]
    }
}
