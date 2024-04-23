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
    _writer: ValueWriter<BufWriter<OwnedWriteHalf>>, // we need to keep this alive
) {
    let mut reader = CommandReader::new(reader);
    let (tx, mut rx) = mpsc::unbounded_channel();

    tokio::spawn(async move {
        while rx.recv().await.is_some() {
            // do nothing — we don't send replies to replica commands
        }
    });

    loop {
        match reader.read().await {
            Ok(command) => {
                if let Err(err) = server.send(command, tx.clone()) {
                    println!("failed to send command to server: {:?}", err);
                    break;
                }
            }
            Err(CommandReadError::Invalid(values)) => {
                println!("ignoring invalid command from master: {:?}", values);
            }
            Err(CommandReadError::Stop) => {
                println!("fatal error reading command from master");
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
    address: String,
    values_sender: mpsc::UnboundedSender<Value>,
}

impl ReplicationManager {
    pub fn new() -> Self {
        Self { replicas: vec![] }
    }

    pub fn add(&mut self, address: String, values_sender: mpsc::UnboundedSender<Value>) {
        self.replicas.push(Replica {
            address,
            values_sender,
        });
    }

    pub fn replicate(&mut self, command: &Command) {
        if command.is_write() {
            self.replicas.retain(|r| send_to_replica(r, command));
        }
    }
}

fn send_to_replica(replica: &Replica, command: &Command) -> bool {
    let channel = &replica.values_sender;
    let address = &replica.address;
    let value = command.to_value();

    match channel.send(value) {
        Ok(_) => true,
        Err(err) => {
            println!("failed to replicate command to {}: {}", address, err);
            false
        }
    }
}
