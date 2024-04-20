use crate::protocol::{RedisCommand, RedisValue};

pub fn to_value(command: &RedisCommand) -> RedisValue {
    match command {
        RedisCommand::Ping { message } => match message {
            Some(message) => RedisValue::command("PING", &[message]),
            None => RedisValue::Array(vec![RedisValue::bulk_string("PING")]),
        },
        RedisCommand::Echo { message } => RedisValue::command("ECHO", &[message]),
        RedisCommand::Get { key } => RedisValue::command("GET", &[key]),
        RedisCommand::Set {
            key,
            value,
            expiry: None,
        } => RedisValue::command("SET", &[key, value]),
        RedisCommand::Set {
            key,
            value,
            expiry: Some(expiry),
        } => {
            let px = "PX".as_bytes().to_vec();
            let expiry = expiry.to_string().as_bytes().to_vec();
            RedisValue::command("SET", &[key, value, &px, &expiry])
        }
        RedisCommand::Info { sections } => RedisValue::command(
            "INFO",
            &sections.iter().map(|x| x.as_ref()).collect::<Vec<_>>(),
        ),
        RedisCommand::Replconf { key, value } => RedisValue::command("REPLCONF", &[key, value]),
        RedisCommand::Psync {
            master_replid,
            master_repl_offset,
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

            RedisValue::command("PSYNC", &[replid, &offset.as_bytes().to_vec()])
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::protocol::{RedisCommand, RedisValue};

    #[test]
    fn ping_no_message() {
        let command = RedisCommand::Ping { message: None };
        assert_to_value(command, &["PING"]);
    }

    #[test]
    fn ping_with_message() {
        let command = RedisCommand::Ping {
            message: Some("message".as_bytes().to_vec()),
        };
        assert_to_value(command, &["PING", "message"])
    }

    #[test]
    fn echo() {
        let command = RedisCommand::Echo {
            message: "message".as_bytes().to_vec(),
        };
        assert_to_value(command, &["ECHO", "message"]);
    }

    #[test]
    fn get() {
        let command = RedisCommand::Get {
            key: "key".as_bytes().to_vec(),
        };
        assert_to_value(command, &["GET", "key"]);
    }

    #[test]
    fn set_no_expiry() {
        let command = RedisCommand::Set {
            key: "key".as_bytes().to_vec(),
            value: "value".as_bytes().to_vec(),
            expiry: None,
        };
        assert_to_value(command, &["SET", "key", "value"]);
    }

    #[test]
    fn set_with_expiry() {
        let command = RedisCommand::Set {
            key: "key".as_bytes().to_vec(),
            value: "value".as_bytes().to_vec(),
            expiry: Some(1000),
        };
        assert_to_value(command, &["SET", "key", "value", "PX", "1000"]);
    }

    #[test]
    fn info() {
        let command = RedisCommand::Info {
            sections: vec!["replication".as_bytes().to_vec()],
        };
        assert_to_value(command, &["INFO", "replication"]);
    }

    #[test]
    fn replconf() {
        let command = RedisCommand::Replconf {
            key: "key".as_bytes().to_vec(),
            value: "value".as_bytes().to_vec(),
        };
        assert_to_value(command, &["REPLCONF", "key", "value"]);
    }

    #[test]
    fn psync_no_options() {
        let command = RedisCommand::Psync {
            master_replid: None,
            master_repl_offset: None,
        };

        assert_to_value(command, &["PSYNC", "?", "-1"]);
    }

    #[test]
    fn psync_with_options() {
        let command = RedisCommand::Psync {
            master_replid: Some("id".as_bytes().to_vec()),
            master_repl_offset: Some(0),
        };

        assert_to_value(command, &["PSYNC", "id", "0"]);
    }

    fn assert_to_value(command: RedisCommand, expected: &[&str]) {
        let value = command.to_value();

        let expected = RedisValue::Array(
            expected
                .iter()
                .map(|part| RedisValue::bulk_string(part))
                .collect(),
        );

        assert_eq!(value, expected)
    }
}
