mod database;
mod handler;
mod listener;
mod replication;

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Context;
use tokio::io;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::protocol::{RedisCommand, RedisError, RedisValue};

use self::database::DatabaseHandle;
use self::replication::ReplicationManagerHandle;

#[derive(Clone)]
pub struct RedisServerOptions {
    pub port: u16,
    pub replica_of: Option<String>,
}

#[derive(Clone)]
pub struct RedisServer {
    options: RedisServerOptions,
    database: DatabaseHandle,
    replication_manager: ReplicationManagerHandle,
}

impl RedisServer {
    pub async fn start(options: RedisServerOptions) -> Result<(), RedisError> {
        let database = database::start();
        let replication_manager = replication::start(options.replica_of.clone()).await?;

        let server = Self {
            options,
            database,
            replication_manager,
        };

        listener::start(server)
            .await
            .context("failed to join listener task")?
    }

    pub fn port(&self) -> u16 {
        self.options.port
    }

    pub fn database(&self) -> &DatabaseHandle {
        &self.database
    }

    pub fn replication_manager(&self) -> &ReplicationManagerHandle {
        &self.replication_manager
    }

    pub fn receive_command(&self, command: RedisCommand, reply_to: UnboundedSender<RedisValue>) {
        let (tx, rx) = oneshot::channel();
        let database = self.database.clone();

        tokio::spawn(async move {
            let response = database.handle(command).await;
            let _ = tx.send(response);
        });

        rx
    }

    async fn handle(&self, command: RedisCommand) -> Vec<RedisValue> {
        match command {
            RedisCommand::Ping { message } => self.ping(message),
            RedisCommand::Echo { message } => self.echo(message),
            RedisCommand::Get { key } => self.get(key).await,
            RedisCommand::Set { key, value, expiry } => self.set(key, value, expiry).await,
            RedisCommand::Info { .. } => self.info(),
            _ => vec![RedisValue::simple_error("ERR unhandled command")],
        }
    }

    fn ping(&self, message: Option<Vec<u8>>) -> Vec<RedisValue> {
        let response = match message {
            None => RedisValue::simple_string("PONG"),
            Some(message) => RedisValue::BulkString(message),
        };

        vec![response]
    }

    fn echo(&self, message: Vec<u8>) -> Vec<RedisValue> {
        vec![RedisValue::BulkString(message)]
    }

    async fn get(&self, key: Vec<u8>) -> Vec<RedisValue> {
        match self.database().get(key).await {
            Some(value) => vec![RedisValue::BulkString(value)],
            None => vec![RedisValue::NullBulkString],
        }
    }

    async fn set(&self, key: Vec<u8>, value: Vec<u8>, expiry: Option<u64>) -> Vec<RedisValue> {
        let expiry = expiry.map(|px| Duration::from_millis(px));
        self.database().set(key, value, expiry).await;
        vec![RedisValue::simple_string("OK")]
    }

    fn info(&self) -> Vec<RedisValue> {
        let mut info = HashMap::new();

        if self.replication_manager().is_slave() {
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

        vec![RedisValue::BulkString(result.into_bytes())]
    }
}
