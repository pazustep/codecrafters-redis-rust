use super::RedisServer;
use crate::protocol::{RedisCommand, RedisError, RedisValue, ValueReader, ValueWriter};
use std::io;
use tokio::io::{AsyncWrite, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::{JoinHandle, JoinSet};

pub fn start(server: RedisServer) -> JoinHandle<Result<(), RedisError>> {
    tokio::spawn(async move {
        listen(server)
            .await
            .map_err(|err| RedisError::Unexpected(anyhow::Error::new(err)))
    })
}

async fn listen(server: RedisServer) -> io::Result<()> {
    let addr = format!("127.0.0.1:{}", server.port());
    let listener = tokio::net::TcpListener::bind(addr).await?;
    println!("listening on {}", listener.local_addr().unwrap());

    let mut handlers = JoinSet::new();

    loop {
        match listener.accept().await {
            Ok((socket, peer_addr)) => {
                println!("accepted new connection from {}", peer_addr);
                let server = server.clone();
                handlers.spawn(async move { handle_client(server, socket).await });
            }
            Err(err) => {
                println!("error accepting new connection; shutting down: {}", err);
                break;
            }
        }
    }

    // await all pending handlers
    while handlers.join_next().await.is_some() {}

    println!("exiting listener task");
    Ok(())
}

async fn handle_client(server: RedisServer, socket: TcpStream) {
    let (reader, writer) = socket.into_split();
    let writer = value_writer_task(writer);
    let handler = super::handler::start(server, writer.clone());

    let read = BufReader::new(reader);
    let mut parser = RedisValueParser::new(read);

    loop {
        match parser.read_value().await {
            Ok(value) => match RedisCommand::try_from(value) {
                Ok(command) => {
                    if handler.send(command).is_err() {
                        println!("error sending command to handler; exiting");
                        break;
                    }
                }
                Err(error) => {
                    println!("invalid command: {}", error);
                    let value = format!("invalid command: {}", error);
                    let value = RedisValue::SimpleError(value);

                    if let Err(error) = writer.send(value) {
                        println!("error sending error response to writer; exiting: {}", error);
                        break;
                    }
                }
            },
            Err(err) => {
                println!("error reading command; exiting: {}", err);
                break;
            }
        }
    }
}

fn value_writer_task<W>(writer: W) -> UnboundedSender<RedisValue>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    let mut writer = RedisValueWriter::new(BufWriter::new(writer));
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

    tokio::spawn(async move {
        while let Some(value) = rx.recv().await {
            if writer.write(&value).await.is_err() {
                println!("error writing value; exiting");
                break;
            }
        }

        println!("EOF reached; stopping writer task");
    });

    tx
}
