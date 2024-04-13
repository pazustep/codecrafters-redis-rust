pub fn main() -> Result<(), RedisError> {
    let args = parse_args();
    let server = RedisServer::new(args);
    server.run()
}

#[derive(Clone, Copy)]
enum ArgState {
    Normal,
    Port,
    ReplicaHost,
    ReplicaPort,
}

fn parse_args() -> RedisArguments {
    let mut state = ArgState::Normal;
    let mut port: Option<u16> = None;
    let mut replica_host: Option<String> = None;
    let mut replica_port: Option<u16> = None;

    for arg in std::env::args().skip(1) {
        match (state, arg.as_str()) {
            (ArgState::Normal, "--port") => state = ArgState::Port,
            (ArgState::Normal, "--replicaof") => state = ArgState::ReplicaHost,
            (ArgState::Port, value) => {
                port = value.parse().ok();
                state = ArgState::Normal;
            }
            (ArgState::ReplicaHost, value) => {
                replica_host = Some(value.to_string());
                state = ArgState::ReplicaPort;
            }
            (ArgState::ReplicaPort, value) => {
                replica_port = value.parse().ok();
                state = ArgState::Normal;
            }
            (_, value) => {
                eprintln!("ignoring invalid argument: {}", value)
            }
        }
    }

    let replica_of = replica_host.map(|host| {
        let port = replica_port.unwrap_or(DEFAULT_PORT);
        format!("{}:{}", host, port)
    });

    RedisArguments {
        port: port.unwrap_or(DEFAULT_PORT),
        replica_of,
    }
}

static DEFAULT_PORT: u16 = 6379;
