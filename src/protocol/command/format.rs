use crate::protocol::{Command, Value};

pub fn to_value(command: &Command) -> Value {
    match command {
        Command::Ping { message, .. } => match message {
            Some(message) => Value::command("PING", &[message]),
            None => Value::command("PING", &[]),
        },
        Command::Echo { message, .. } => Value::command("ECHO", &[message]),
        Command::Get { key, .. } => Value::command("GET", &[key]),
        Command::Set {
            key,
            value,
            expiry: None,
            ..
        } => Value::command("SET", &[key, value]),
        Command::Set {
            key,
            value,
            expiry: Some(expiry),
            ..
        } => {
            let px = "PX".as_bytes().to_vec();
            let expiry = expiry.as_millis().to_string().into_bytes();
            Value::command("SET", &[key, value, &px, &expiry])
        }
        Command::Info { sections, .. } => {
            Value::command("INFO", &sections.iter().collect::<Vec<_>>())
        }
        Command::Replconf { key, value, .. } => Value::command("REPLCONF", &[key, value]),
        Command::Psync {
            master_replid,
            master_repl_offset,
            ..
        } => {
            let empty_repl_id = "?".as_bytes().to_vec();

            let replid = match master_replid {
                Some(master_replid) => master_replid,
                None => &empty_repl_id,
            };

            let offset = match master_repl_offset {
                Some(offset) => offset.to_string(),
                None => "-1".to_string(),
            };

            Value::command("PSYNC", &[replid, &offset.as_bytes().to_vec()])
        }
        Command::Wait {
            replicas, timeout, ..
        } => {
            let replicas = replicas.to_string().into_bytes();
            let timeout = timeout.to_string().into_bytes();
            Value::command("WAIT", &[&replicas, &timeout])
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::protocol::{Command, Value};
    use std::time::Duration;

    #[test]
    fn ping_no_message() {
        let command = Command::Ping {
            size: 0,
            message: None,
        };
        assert_to_value(command, &["PING"]);
    }

    #[test]
    fn ping_with_message() {
        let command = Command::Ping {
            size: 0,
            message: Some("message".as_bytes().to_vec()),
        };
        assert_to_value(command, &["PING", "message"])
    }

    #[test]
    fn echo() {
        let command = Command::Echo {
            size: 0,
            message: "message".as_bytes().to_vec(),
        };
        assert_to_value(command, &["ECHO", "message"]);
    }

    #[test]
    fn get() {
        let command = Command::Get {
            size: 0,
            key: "key".as_bytes().to_vec(),
        };
        assert_to_value(command, &["GET", "key"]);
    }

    #[test]
    fn set_no_expiry() {
        let command = Command::Set {
            size: 0,
            key: "key".as_bytes().to_vec(),
            value: "value".as_bytes().to_vec(),
            expiry: None,
        };
        assert_to_value(command, &["SET", "key", "value"]);
    }

    #[test]
    fn set_with_expiry() {
        let command = Command::Set {
            size: 0,
            key: "key".as_bytes().to_vec(),
            value: "value".as_bytes().to_vec(),
            expiry: Some(Duration::from_millis(1000)),
        };
        assert_to_value(command, &["SET", "key", "value", "PX", "1000"]);
    }

    #[test]
    fn info() {
        let command = Command::Info {
            size: 0,
            sections: vec!["replication".as_bytes().to_vec()],
        };
        assert_to_value(command, &["INFO", "replication"]);
    }

    #[test]
    fn replconf() {
        let command = Command::Replconf {
            size: 0,
            key: "key".as_bytes().to_vec(),
            value: "value".as_bytes().to_vec(),
        };
        assert_to_value(command, &["REPLCONF", "key", "value"]);
    }

    #[test]
    fn psync_no_options() {
        let command = Command::Psync {
            size: 0,
            master_replid: None,
            master_repl_offset: None,
        };

        assert_to_value(command, &["PSYNC", "?", "-1"]);
    }

    #[test]
    fn psync_with_options() {
        let command = Command::Psync {
            size: 0,
            master_replid: Some("id".as_bytes().to_vec()),
            master_repl_offset: Some(0),
        };

        assert_to_value(command, &["PSYNC", "id", "0"]);
    }

    fn assert_to_value(command: Command, expected: &[&str]) {
        let value = command.to_value();

        let expected = Value::command_str(expected[0], &expected[1..]);
        assert_eq!(value, expected)
    }
}
