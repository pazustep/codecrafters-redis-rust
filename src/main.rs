mod protocol;

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

fn handle_connection(mut stream: TcpStream) {
    loop {
        let result = {
            let mut reader = BufReader::new(&stream);
            protocol::read_array_of_bulk_strings(&mut reader)
        };

        match result {
            Ok(command) => process_command(command, &mut stream),
            Err(e) => {
                eprintln!("failed to read command: {}", e);
                break;
            }
        }
    }
}

fn process_command<W: Write>(command: Vec<String>, output: &mut W) {
    match command[0].to_uppercase().as_str() {
        "PING" => {
            let response = "+PONG\r\n";
            output.write_all(response.as_bytes()).unwrap();
            output.flush().unwrap()
        }
        _ => {
            println!("unknown command: {}", command[0]);
        }
    }
}
