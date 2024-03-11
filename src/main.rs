mod protocol;

use anyhow::Result;
use std::{
    io::{BufReader, Write},
    net::{TcpListener, TcpStream},
};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                std::thread::spawn(move || handle_connection(stream));
            }
            Err(e) => {
                eprintln!("error: {}", e);
            }
        }
    }
}

fn handle_connection(mut stream: TcpStream) -> Result<()> {
    loop {
        let command = {
            let mut reader = BufReader::new(&stream);
            protocol::read_array_of_bulk_strings(&mut reader)
        }?;

        process_command(command, &mut stream)?;
    }
}

fn process_command<W: Write>(command: Vec<String>, output: &mut W) -> Result<()> {
    match command[0].to_uppercase().as_str() {
        "PING" => {
            let response = "+PONG\r\n";
            output.write_all(response.as_bytes())?;
            output.flush()?;
        }

        "ECHO" => {
            let argument = command[1].as_str();
            protocol::write_bulk_string(argument, output)?;
        }

        _ => {
            println!("unknown command: {}", command[0]);
        }
    };

    Ok(())
}
