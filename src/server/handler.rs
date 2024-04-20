use std::{collections::HashMap, time::Duration};

use super::RedisServer;
use crate::protocol::{RedisCommand, RedisValue};
use tokio::sync::mpsc;

pub fn start(
    server: RedisServer,
) -> mpsc::UnboundedSender<(RedisCommand, mpsc::UnboundedSender<RedisValue>)> {
    let (tx, mut rx) =
        mpsc::unbounded_channel::<(RedisCommand, mpsc::UnboundedSender<RedisValue>)>();

    tokio::spawn(async move {
        let handler = CommandHandler::new(server);
        println!("started command handler task");

        'outer: while let Some((command, reply_to)) = rx.recv().await {
            let response = handler.handle(command).await;

            for value in response {
                if reply_to.send(value).is_err() {
                    println!("error sending value to writer; exiting");
                    break 'outer;
                }
            }
        }

        println!("command handler task finished");
    });

    tx
}

struct CommandHandler {
    server: RedisServer,
}

impl CommandHandler {
    fn new(server: RedisServer) -> Self {
        Self { server }
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
        match self.server.database().get(key).await {
            Some(value) => vec![RedisValue::BulkString(value)],
            None => vec![RedisValue::NullBulkString],
        }
    }

    async fn set(&self, key: Vec<u8>, value: Vec<u8>, expiry: Option<u64>) -> Vec<RedisValue> {
        let expiry = expiry.map(|px| Duration::from_millis(px));
        self.server.database().set(key, value, expiry).await;
        vec![RedisValue::simple_string("OK")]
    }

    fn info(&self) -> Vec<RedisValue> {
        let mut info = HashMap::new();

        if self.server.replication_manager().is_slave() {
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
