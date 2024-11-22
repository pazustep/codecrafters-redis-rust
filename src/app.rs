use crate::server::ServerOptions;

#[derive(Clone, Copy)]
enum ArgState {
    Normal,
    Port,
    ReplicaOf,
}

pub fn parse_options() -> ServerOptions {
    let mut state = ArgState::Normal;
    let mut port: Option<u16> = None;
    let mut replica_of: Option<String> = None;

    for arg in std::env::args().skip(1) {
        match (state, arg.as_str()) {
            (ArgState::Normal, "--port") => state = ArgState::Port,
            (ArgState::Normal, "--replicaof") => state = ArgState::ReplicaOf,
            (ArgState::Port, value) => {
                port = value.parse().ok();
                state = ArgState::Normal;
            }
            (ArgState::ReplicaOf, value) => {
                replica_of = Some(value.replace(' ', ":"));
                state = ArgState::Normal;
            }
            (_, value) => {
                eprintln!("ignoring invalid argument: {}", value)
            }
        }
    }

    ServerOptions {
        port: port.unwrap_or(DEFAULT_PORT),
        replica_of,
    }
}

static DEFAULT_PORT: u16 = 6379;
