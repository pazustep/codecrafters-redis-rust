use crate::protocol::{Command, Value, ValueReadError, ValueReader, ValueWriter};
use crate::server::{ServerHandle, ServerOptions};

use std::io;
use tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite, BufReader, BufWriter};
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
    let (socket_reader, socket_writer) = socket.into_split();
    let (values_sender, values_receiver) = mpsc::unbounded_channel::<Vec<Value>>();

    let reader_handle =
        tokio::spawn(
            async move { handle_client_reader(server, socket_reader, values_sender).await },
        );

    let writer_handle =
        tokio::spawn(async move { handle_client_writer(socket_writer, values_receiver).await });

    tokio::select! {
        _ = reader_handle => {}
        _ = writer_handle => {}
    }
}

async fn handle_client_reader<R>(
    server: ServerHandle,
    socket_reader: R,
    values_sender: mpsc::UnboundedSender<Vec<Value>>,
) where
    R: AsyncRead + Unpin,
{
    let mut reader = ValueReader::new(BufReader::new(socket_reader));

    loop {
        match read_command(&mut reader).await {
            ReadCommandResult::Success(command) => {
                if let Err(err) = server.send(command, values_sender.clone()) {
                    println!("failed to send command to server: {:?}", err);
                    break;
                }
            }
            ReadCommandResult::Invalid(values) => {
                if let Err(err) = values_sender.send(values) {
                    println!("failed to send invalid response to client: {:?}", err);
                    break;
                }
            }
            ReadCommandResult::Stop => {
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

enum ReadCommandResult {
    Success(Command),
    Invalid(Vec<Value>),
    Stop,
}

async fn read_command<R>(reader: &mut ValueReader<R>) -> ReadCommandResult
where
    R: AsyncBufRead + Unpin,
{
    match reader.read().await {
        Ok(value) => parse_command(value),
        Err(ValueReadError::EndOfInput) => ReadCommandResult::Stop,
        Err(ValueReadError::Invalid { message, .. }) => {
            let message = format!("invalid RESP value: {}", message);
            ReadCommandResult::Invalid(vec![Value::SimpleError(message)])
        }
        Err(err) => {
            println!("I/O error reading command: {}", err);
            ReadCommandResult::Stop
        }
    }
}

fn parse_command(value: Value) -> ReadCommandResult {
    match Command::try_from(value) {
        Ok(command) => ReadCommandResult::Success(command),
        Err(err) => {
            let message = format!("{}", err);
            ReadCommandResult::Invalid(vec![Value::SimpleError(message)])
        }
    }
}
