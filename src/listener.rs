use crate::protocol::{Command, CommandReadError, CommandReader, Value, ValueReader, ValueWriter};
use crate::server::{ServerHandle, ServerOptions};
use std::{io, net::SocketAddr};
use tokio::io::{AsyncRead, AsyncWrite, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

pub fn start(options: ServerOptions, server: ServerHandle) -> JoinHandle<Result<(), io::Error>> {
    tokio::spawn(async move { listen(options, server).await })
}

async fn listen(options: ServerOptions, server: ServerHandle) -> io::Result<()> {
    let addr = format!("127.0.0.1:{}", options.port);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let mut conn_counter = 0;
    println!("listening on {}", listener.local_addr().unwrap());

    loop {
        match listener.accept().await {
            Ok((socket, peer_addr)) => {
                conn_counter += 1;
                println!("accepted connection #{} from {}", conn_counter, peer_addr);

                let server = server.clone();

                tokio::spawn(async move {
                    handle_client(server, socket).await;
                    println!("closing connection #{} from {}", conn_counter, peer_addr)
                });
            }
            Err(err) => {
                println!("error accepting new connection; shutting down: {}", err);
                break;
            }
        }
    }

    Ok(())
}

async fn handle_client(server: ServerHandle, socket: TcpStream) {
    let address = socket.peer_addr().unwrap();
    let (socket_reader, socket_writer) = socket.into_split();
    let (values_sender, values_receiver) = mpsc::unbounded_channel::<Vec<Value>>();

    let reader_handle = tokio::spawn(async move {
        handle_client_reader(server, address, socket_reader, values_sender).await
    });

    let writer_handle =
        tokio::spawn(async move { handle_client_writer(socket_writer, values_receiver).await });

    tokio::select! {
        _ = reader_handle => {}
        _ = writer_handle => {}
    }
}

async fn handle_client_reader<R>(
    server: ServerHandle,
    address: SocketAddr,
    socket_reader: R,
    values_sender: mpsc::UnboundedSender<Vec<Value>>,
) where
    R: AsyncRead + Unpin,
{
    let mut reader = CommandReader::new(ValueReader::new(BufReader::new(socket_reader)));

    loop {
        match reader.read().await {
            Ok(command) => {
                if let Err(err) = server.send(command.clone(), values_sender.clone()) {
                    println!("failed to send command to server: {:?}", err);
                    break;
                }

                if let Command::Psync { .. } = command {
                    server.add_replica(address, values_sender.clone());
                }
            }
            Err(CommandReadError::Invalid(values)) => {
                if let Err(err) = values_sender.send(values) {
                    println!("failed to send invalid response to client: {:?}", err);
                    break;
                }
            }
            Err(CommandReadError::Stop(cause)) => {
                if let Some(cause) = cause {
                    println!("fatal error reading command: {}", cause);
                }

                break;
            }
        }
    }
}

async fn handle_client_writer<W>(
    socket_writer: W,
    mut values_receiver: mpsc::UnboundedReceiver<Vec<Value>>,
) where
    W: AsyncWrite + Unpin,
{
    let mut writer = ValueWriter::new(BufWriter::new(socket_writer));

    while let Some(values) = values_receiver.recv().await {
        for value in values {
            if let Err(error) = writer.write(&value).await {
                println!("error writing value to client: {}", error);
                return;
            }
        }
    }
}
