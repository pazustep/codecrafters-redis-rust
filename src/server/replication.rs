use crate::protocol::{RedisError, RedisValue, RedisValueParser, RedisValueWriter};
use anyhow::Context;
use tokio::{
    io::{BufReader, BufWriter},
    net::TcpStream,
};

pub async fn start(master: Option<String>) -> Result<ReplicationManagerHandle, RedisError> {
    if let Some(ref master) = master {
        start_replication(master).await?;
    };

    Ok(ReplicationManagerHandle { master })
}

#[derive(Clone)]
pub struct ReplicationManagerHandle {
    master: Option<String>,
}

impl ReplicationManagerHandle {
    pub fn is_slave(&self) -> bool {
        self.master.is_some()
    }
}

async fn start_replication(master: &str) -> Result<(), RedisError> {
    println!("starting replicating from {}", master);

    let mut stream = TcpStream::connect(master)
        .await
        .context(format!("failed to connect to master {}", master))?;

    let (reader, writer) = stream.split();
    let mut reader = RedisValueParser::new(BufReader::new(reader));
    let mut writer = RedisValueWriter::new(BufWriter::new(writer));

    let handshake = vec![
        RedisValue::command_str("PING", &[]),
        RedisValue::command_str("REPLCONF", &["listening-port", "0"]),
        RedisValue::command_str("REPLCONF", &["capa", "psync2"]),
        RedisValue::command_str("PSYNC", &["?", "-1"]),
    ];

    for command in handshake {
        writer.write(&command).await?;
        let reply = reader.read_value().await?;
        println!("master responded {:?} with {:?}", command, reply);
    }

    // do nothing, for now
    let _ = reader.read_bytes().await?;
    println!("received RDB file");

    Ok(())
}
