mod database;
mod replication;

use crate::protocol::{Command, Value};
use database::Database;
use std::{collections::HashMap, time::Duration};
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct ServerOptions {
    pub port: u16,
    pub replica_of: Option<String>,
}

#[derive(Clone)]
pub struct ServerHandle {
    sender: mpsc::UnboundedSender<CommandEnvelope>,
}

#[derive(Debug, thiserror::Error)]
#[error("failed to send command to server: {0:?}")]
pub struct ServerSendError(Command);

impl ServerHandle {
    pub fn send(
        &self,
        command: Command,
        reply_to: mpsc::UnboundedSender<Vec<Value>>,
    ) -> Result<(), ServerSendError> {
        let envelope = CommandEnvelope { command, reply_to };

        self.sender
            .send(envelope)
            .map_err(|err| ServerSendError(err.0.command))
    }
}

struct CommandEnvelope {
    command: Command,
    reply_to: mpsc::UnboundedSender<Vec<Value>>,
}

pub fn start(options: ServerOptions) -> ServerHandle {
    let (tx, rx) = mpsc::unbounded_channel::<CommandEnvelope>();
    let server = ServerHandle { sender: tx };

    replication::start(options.clone(), server.clone());
    tokio::spawn(async move { command_loop(options, rx).await });

    server
}

async fn command_loop(
    options: ServerOptions,
    mut receiver: mpsc::UnboundedReceiver<CommandEnvelope>,
) {
    let mut server = Server::new(options);

    loop {
        match receiver.recv().await {
            None => {
                println!("server channel closed; exiting task");
                break;
            }
            Some(envelope) => {
                let response = server.handle(envelope.command);
                if envelope.reply_to.send(response).is_err() {
                    println!("failed to send response to client; ignoring");
                }
            }
        }
    }
}

struct Server {
    options: ServerOptions,
    database: Database,
}

impl Server {
    pub fn new(options: ServerOptions) -> Self {
        Self {
            options,
            database: Database::new(),
        }
    }

    fn handle(&mut self, command: Command) -> Vec<Value> {
        match command {
            Command::Ping { message } => self.ping(message),
            Command::Echo { message } => self.echo(message),
            Command::Get { key } => self.get(key),
            Command::Set { key, value, expiry } => self.set(key, value, expiry),
            Command::Info { .. } => self.info(),
            _ => vec![Value::simple_error("ERR unhandled command")],
        }
    }

    fn ping(&self, message: Option<Vec<u8>>) -> Vec<Value> {
        let response = match message {
            None => Value::simple_string("PONG"),
            Some(message) => Value::BulkString(message),
        };

        vec![response]
    }

    fn echo(&self, message: Vec<u8>) -> Vec<Value> {
        vec![Value::BulkString(message)]
    }

    fn get(&mut self, key: Vec<u8>) -> Vec<Value> {
        match self.database.get(key) {
            Some(value) => vec![Value::BulkString(value)],
            None => vec![Value::NullBulkString],
        }
    }

    fn set(&mut self, key: Vec<u8>, value: Vec<u8>, expiry: Option<Duration>) -> Vec<Value> {
        self.database.set(key, value, expiry);
        vec![Value::simple_string("OK")]
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

        vec![Value::BulkString(result.into_bytes())]
    }
}
