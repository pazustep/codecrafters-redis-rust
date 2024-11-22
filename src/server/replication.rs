use std::net::SocketAddr;

use crate::{
    protocol::{Command, CommandReadError, CommandReader, Value, ValueReader, ValueWriter},
    server::{ServerHandle, ServerOptions},
};
use tokio::{
    io::{BufReader, BufWriter},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
    sync::{mpsc, oneshot},
};

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ReplicationError(String);

pub fn start(
    options: ServerOptions,
    server: ServerHandle,
) -> oneshot::Receiver<Result<(), ReplicationError>> {
    let (tx, rx) = oneshot::channel();

    if let Some(master) = options.replica_of {
        tokio::spawn(async move {
            match init(master, options.port).await {
                Err(err) => {
                    tx.send(Err(err)).unwrap();
                }
                Ok((reader, writer)) => {
                    tx.send(Ok(())).unwrap();
                    replication_loop(server, reader, writer).await;
                }
            };
        });
    } else {
        tx.send(Ok(())).unwrap();
    }

    rx
}

async fn init(
    master: String,
    port: u16,
) -> Result<
    (
        ValueReader<BufReader<OwnedReadHalf>>,
        ValueWriter<BufWriter<OwnedWriteHalf>>,
    ),
    ReplicationError,
> {
    let stream = TcpStream::connect(master)
        .await
        .map_err(|err| ReplicationError(format!("failed to connect to master: {}", err)))?;

    let (reader, writer) = stream.into_split();
    let mut reader = ValueReader::new(BufReader::new(reader));
    let mut writer = ValueWriter::new(BufWriter::new(writer));

    let port = port.to_string();
    let commands = vec![
        Value::command_str("PING", &[]),
        Value::command_str("REPLCONF", &["listening-port", &port]),
        Value::command_str("REPLCONF", &["capa", "psync2"]),
        Value::command_str("PSYNC", &["?", "-1"]),
    ];

    for command in commands {
        writer
            .write(&command)
            .await
            .map_err(|_| ReplicationError("failed to write command".to_string()))?;

        reader
            .read()
            .await
            .map_err(|_| ReplicationError("failed to read response".to_string()))?;
    }

    let rdb = reader
        .read_bytes()
        .await
        .map_err(|err| ReplicationError(format!("error reading RDB from master: {}", err)))?;

    println!("received RDB transfer: {:?}", rdb);

    Ok((reader, writer))
}

async fn replication_loop(
    server: ServerHandle,
    reader: ValueReader<BufReader<OwnedReadHalf>>,
    mut writer: ValueWriter<BufWriter<OwnedWriteHalf>>, // we need to keep this alive
) {
    let mut reader = CommandReader::new(reader);

    // a channel that will ignore all received values
    let (black_hole_sender, mut black_hole_receiver) = mpsc::unbounded_channel();

    // a channel to write values to the replication connection
    let (values_sender, mut values_receiver) = mpsc::unbounded_channel();

    tokio::spawn(async move {
        while black_hole_receiver.recv().await.is_some() {
            // do nothing — we don't send replies to replica commands
        }
    });

    tokio::spawn(async move {
        while let Some(values) = values_receiver.recv().await {
            for value in values {
                if let Err(err) = writer.write(&value).await {
                    println!("failed to write reply to master: {}", err);
                    return;
                }
            }
        }
    });

    loop {
        match reader.read().await {
            Ok(command) => {
                let reply_to = match &command {
                    Command::Replconf { key, .. }
                        if String::from_utf8_lossy(key).to_uppercase() == "GETACK" =>
                    {
                        println!("using real response channel for command {:?}", command);
                        values_sender.clone()
                    }
                    _ => black_hole_sender.clone(),
                };

                if let Err(err) = server.send(command, reply_to) {
                    println!("failed to send command to server: {:?}", err);
                    break;
                }
            }
            Err(CommandReadError::Invalid(values)) => {
                println!("ignoring invalid command from master: {:?}", values);
            }
            Err(CommandReadError::Stop(cause)) => {
                if let Some(cause) = cause {
                    println!("error reading command from master: {}", cause);
                }
                break;
            }
        }
    }

    println!("exiting replication loop");
}

pub struct ReplicationManager {
    replicas: Vec<Replica>,
}

struct Replica {
    address: SocketAddr,
    values_sender: mpsc::UnboundedSender<Vec<Value>>,
}

impl ReplicationManager {
    pub fn new() -> Self {
        Self { replicas: vec![] }
    }

    pub fn add(&mut self, address: SocketAddr, values_sender: mpsc::UnboundedSender<Vec<Value>>) {
        self.replicas.push(Replica {
            address,
            values_sender,
        });
        println!("added replica: {}", address);
    }

    pub fn replicate(&mut self, command: &Command) {
        if command.is_write() {
            self.replicas.retain(|r| send_to_replica(r, command));
        }
    }

    pub fn connected_replicas(&self) -> i64 {
        self.replicas.len() as i64
    }
}

fn send_to_replica(replica: &Replica, command: &Command) -> bool {
    let channel = &replica.values_sender;
    let address = &replica.address;
    let value = command.to_value();

    match channel.send(vec![value]) {
        Ok(_) => true,
        Err(err) => {
            println!("failed to replicate command to {}: {}", address, err);
            false
        }
    }
}
