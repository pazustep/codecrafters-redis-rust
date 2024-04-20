use crate::protocol::{RedisCommand, RedisError, RedisValue};
use std::collections::VecDeque;

pub fn from_value(value: RedisValue) -> Result<RedisCommand, RedisError> {
    match value {
        RedisValue::Array(values) => from_values(values),
        value => {
            let message = format!("unexpected RESP value: {:?}", value);
            Err(RedisError::protocol(&message))
        }
    }
}

fn from_values(values: Vec<RedisValue>) -> Result<RedisCommand, RedisError> {
    if values.is_empty() {
        return Err(RedisError::protocol("empty RESP array"));
    }

    let mut parts = VecDeque::with_capacity(values.len());

    for value in values {
        match value {
            RedisValue::BulkString(bytes) => {
                parts.push_back(bytes);
            }
            value => {
                let message = format!("unexpected RESP value: {:?}", value);
                return Err(RedisError::Protocol(message));
            }
        }
    }

    from_parts(parts)
}

fn from_parts(mut values: VecDeque<Vec<u8>>) -> Result<RedisCommand, RedisError> {
    let command = values.pop_front().unwrap();
    let command = from_utf8(command)?;

    match command.to_uppercase().as_str() {
        "PING" => parse_ping(values),
        "ECHO" => parse_echo(values),
        "GET" => parse_get(values),
        "SET" => parse_set(values),
        "INFO" => parse_info(values),
        "REPLCONF" => parse_replconf(values),
        "PSYNC" => parse_psync(values),
        cmd => invalid_command(cmd),
    }
}

fn parse_ping(mut args: VecDeque<Vec<u8>>) -> Result<RedisCommand, RedisError> {
    let message = args.pop_front();
    Ok(RedisCommand::Ping { message })
}

fn parse_echo(mut args: VecDeque<Vec<u8>>) -> Result<RedisCommand, RedisError> {
    let message = args
        .pop_front()
        .ok_or_else(|| RedisError::wrong_number_of_arguments("ECHO"))?;
    Ok(RedisCommand::Echo { message })
}

fn parse_get(mut args: VecDeque<Vec<u8>>) -> Result<RedisCommand, RedisError> {
    let key = args
        .pop_front()
        .ok_or_else(|| RedisError::wrong_number_of_arguments("GET"))?;

    Ok(RedisCommand::Get { key })
}

fn parse_set(mut args: VecDeque<Vec<u8>>) -> Result<RedisCommand, RedisError> {
    if args.len() < 2 {
        return Err(RedisError::wrong_number_of_arguments("SET"));
    }

    let key = args.pop_front().unwrap();
    let value = args.pop_front().unwrap();
    let expiry = parse_set_args(args)?;

    Ok(RedisCommand::Set { key, value, expiry })
}

fn parse_set_args(mut args: VecDeque<Vec<u8>>) -> Result<Option<u64>, RedisError> {
    match args.pop_front() {
        Some(arg) => match from_utf8(arg)?.to_uppercase().as_str() {
            "PX" => parse_set_expiry(args).map(Some),
            arg => {
                let message = format!("invalid SET argument: {}", arg);
                Err(RedisError::Protocol(message))
            }
        },
        None => Ok(None),
    }
}

fn parse_set_expiry(mut args: VecDeque<Vec<u8>>) -> Result<u64, RedisError> {
    let bytes = args
        .pop_front()
        .ok_or_else(|| RedisError::wrong_number_of_arguments("SET PX"))?;

    let text = from_utf8(bytes)?;

    let expiry = text
        .parse::<u64>()
        .map_err(|_| RedisError::protocol("invalid integer value for SET PX argument"))?;

    Ok(expiry)
}

fn parse_info(args: VecDeque<Vec<u8>>) -> Result<RedisCommand, RedisError> {
    Ok(RedisCommand::Info {
        sections: args.into(),
    })
}

fn parse_replconf(mut args: VecDeque<Vec<u8>>) -> Result<RedisCommand, RedisError> {
    if args.len() < 2 {
        return Err(RedisError::wrong_number_of_arguments("REPLCONF"));
    }

    let key = args.pop_front().unwrap();
    let value = args.pop_front().unwrap();

    Ok(RedisCommand::Replconf { key, value })
}

fn parse_psync(mut args: VecDeque<Vec<u8>>) -> Result<RedisCommand, RedisError> {
    if args.len() < 2 {
        return Err(RedisError::wrong_number_of_arguments("PSYNC"));
    }

    let master_replid = parse_psync_replid(args.pop_front().unwrap());
    let master_repl_offset = parse_psync_offset(args.pop_front().unwrap())?;

    Ok(RedisCommand::Psync {
        master_replid,
        master_repl_offset,
    })
}

fn parse_psync_replid(replid: Vec<u8>) -> Option<Vec<u8>> {
    if replid == "?".as_bytes().to_vec() {
        None
    } else {
        Some(replid)
    }
}

fn parse_psync_offset(offset: Vec<u8>) -> Result<Option<u32>, RedisError> {
    let offset = from_utf8(offset)?;

    let offset = offset.parse::<i64>().map_err(|_| {
        let message = format!("invalid PSYNC offset: {}", offset);
        RedisError::Protocol(message)
    })?;

    if offset < 0 {
        Ok(None)
    } else {
        Ok(Some(offset as u32))
    }
}

fn invalid_command(command: &str) -> Result<RedisCommand, RedisError> {
    let message = format!("invalid command: {}", command);
    Err(RedisError::Protocol(message))
}

fn from_utf8(bytes: Vec<u8>) -> Result<String, RedisError> {
    String::from_utf8(bytes).map_err(|_| RedisError::protocol("invalid UTF-8"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_redis_value() {
        match from_value(RedisValue::NullBulkString) {
            Err(RedisError::Protocol(message)) => {
                assert!(message.starts_with("unexpected RESP value"))
            }
            value => panic!("expected protocol error, got {:?}", value),
        }
    }

    #[test]
    fn empty_array() {
        match from_value(RedisValue::Array(vec![])) {
            Err(RedisError::Protocol(message)) => assert_eq!(message, "empty RESP array"),
            value => panic!("expected protocol error, got {:?}", value),
        }
    }

    #[test]
    fn malformed_array() {
        let value = RedisValue::Array(vec![RedisValue::simple_string("OK")]);
        match from_value(value) {
            Err(RedisError::Protocol(message)) => {
                assert!(message.starts_with("unexpected RESP value"))
            }
            value => panic!("expected protocol error, got {:?}", value),
        }
    }

    #[test]
    fn ping_without_message() {
        let command = RedisCommand::Ping { message: None };
        assert_command_value(command, &["PING"]);
    }

    #[test]
    fn ping_with_message() {
        let command = RedisCommand::Ping {
            message: Some("message".as_bytes().to_vec()),
        };
        assert_command_value(command, &["PING", "message"]);
    }

    #[test]
    fn echo() {
        let command = RedisCommand::Echo {
            message: "message".as_bytes().to_vec(),
        };

        assert_command_value(command, &["ECHO", "message"]);
    }

    #[test]
    fn get() {
        let command = RedisCommand::Get {
            key: "key".as_bytes().to_vec(),
        };

        assert_command_value(command, &["GET", "key"]);
    }

    #[test]
    fn set_without_expiry() {
        let command = RedisCommand::Set {
            key: "key".as_bytes().to_vec(),
            value: "value".as_bytes().to_vec(),
            expiry: None,
        };

        assert_command_value(command, &["SET", "key", "value"]);
    }

    #[test]
    fn set_with_expiry() {
        let command = RedisCommand::Set {
            key: "key".as_bytes().to_vec(),
            value: "value".as_bytes().to_vec(),
            expiry: Some(1000),
        };

        assert_command_value(command, &["SET", "key", "value", "PX", "1000"]);
    }

    #[test]
    fn info() {
        let command = RedisCommand::Info {
            sections: vec![
                "section1".as_bytes().to_vec(),
                "section2".as_bytes().to_vec(),
            ],
        };

        assert_command_value(command, &["INFO", "section1", "section2"]);
    }

    #[test]
    fn replconf() {
        let command = RedisCommand::Replconf {
            key: "key".as_bytes().to_vec(),
            value: "value".as_bytes().to_vec(),
        };

        assert_command_value(command, &["REPLCONF", "key", "value"]);
    }

    #[test]
    fn psync_empty() {
        let command = RedisCommand::Psync {
            master_replid: None,
            master_repl_offset: None,
        };

        assert_command_value(command, &["PSYNC", "?", "-1"]);
    }

    #[test]
    fn psync_present() {
        let command = RedisCommand::Psync {
            master_replid: Some("replid".as_bytes().to_vec()),
            master_repl_offset: Some(100),
        };

        assert_command_value(command, &["PSYNC", "replid", "100"]);
    }

    #[test]
    fn parse_ping_no_message() {
        let command = from_parts(&["PING"]).unwrap();
        assert_eq!(command, RedisCommand::Ping { message: None })
    }

    #[test]
    fn parse_ping_with_message() {
        match from_parts(&["PING", "message"]) {
            Ok(RedisCommand::Ping {
                message: Some(bytes),
            }) => assert_eq!(bytes, "message".as_bytes().to_vec()),
            value => panic!("expected PING, got {:?}", value),
        }
    }

    #[test]
    fn parse_echo_ok() {
        match from_parts(&["ECHO", "message"]) {
            Ok(RedisCommand::Echo { message }) => {
                assert_eq!(message, "message".as_bytes().to_vec())
            }
            value => panic!("expected ECHO message, got {:?}", value),
        }
    }

    #[test]
    fn parse_echo_wrong_args() {
        match from_parts(&["ECHO"]) {
            Err(RedisError::Protocol(_)) => {}
            value => panic!("expected protocol error, got {:?}", value),
        }
    }

    #[test]
    fn parse_get_ok() {
        match from_parts(&["GET", "key"]) {
            Ok(RedisCommand::Get { key }) => assert_eq!(key, "key".as_bytes().to_vec()),
            value => panic!("expected GET key, got {:?}", value),
        }
    }

    #[test]
    fn parse_get_wrong_args() {
        match from_parts(&["GET"]) {
            Err(RedisError::Protocol(_)) => {}
            value => panic!("expected protocol error, got {:?}", value),
        }
    }

    #[test]
    fn parse_set_without_expiry() {
        match from_parts(&["SET", "key", "value"]) {
            Ok(RedisCommand::Set {
                key,
                value,
                expiry: None,
            }) => {
                assert_eq!(key, "key".as_bytes().to_vec());
                assert_eq!(value, "value".as_bytes().to_vec());
            }
            value => panic!("expected SET key value, got {:?}", value),
        }
    }

    #[test]
    fn parse_set_px() {
        match from_parts(&["SET", "key", "value", "PX", "1000"]) {
            Ok(RedisCommand::Set {
                expiry: Some(1000), ..
            }) => {}
            value => panic!("expected SET key value PX 1000, got {:?}", value),
        }
    }

    #[test]
    fn parse_set_px_missing_arg() {
        match from_parts(&["SET", "key", "value", "PX"]) {
            Err(RedisError::Protocol(message)) => {
                assert!(message.starts_with("wrong number of arguments"))
            }
            value => panic!("expected protocol error, got {:?}", value),
        }
    }

    #[test]
    fn parse_set_px_not_utf8() {
        let value = RedisValue::Array(vec![
            RedisValue::bulk_string("SET"),
            RedisValue::bulk_string("key"),
            RedisValue::bulk_string("value"),
            RedisValue::bulk_string("PX"),
            RedisValue::BulkString(vec![0xC3, 0x28]),
        ]);

        match RedisCommand::try_from(value) {
            Err(RedisError::Protocol(message)) => assert_eq!(message, "invalid UTF-8"),
            value => panic!("expected invalid UTF-8, got {:?}", value),
        }
    }

    #[test]
    fn parse_set_px_not_numeric() {
        match from_parts(&["SET", "key", "value", "PX", "abc"]) {
            Err(RedisError::Protocol(message)) => assert!(message.starts_with("invalid integer")),
            value => panic!("expected protocol error, got {:?}", value),
        }
    }

    #[test]
    fn parse_set_unknown_arg() {
        match from_parts(&["SET", "key", "value", "EX"]) {
            Err(RedisError::Protocol(message)) => {
                assert!(message.starts_with("invalid SET argument"))
            }
            value => panic!("expected protocol error, got {:?}", value),
        }
    }

    #[test]
    fn parse_set_wrong_number_of_args() {
        match from_parts(&["SET", "key"]) {
            Err(RedisError::Protocol(message)) => {
                assert!(message.starts_with("wrong number of arguments"))
            }
            value => panic!("expected protocol error, got {:?}", value),
        }
    }

    #[test]
    fn parse_info_one_section() {
        match from_parts(&["INFO", "replication"]) {
            Ok(RedisCommand::Info { sections }) => {
                assert_eq!(sections, vec!["replication".as_bytes().to_vec()])
            }
            value => panic!("expected INFO replication, got {:?}", value),
        }
    }

    #[test]
    fn parse_info_empty_sections() {
        match from_parts(&["INFO"]) {
            Ok(RedisCommand::Info { sections }) => assert!(sections.is_empty()),
            value => panic!("expected INFO, got {:?}", value),
        }
    }

    #[test]
    fn parse_replconf_ok() {
        match from_parts(&["REPLCONF", "key", "value"]) {
            Ok(RedisCommand::Replconf { key, value }) => {
                assert_eq!(key, "key".as_bytes().to_vec());
                assert_eq!(value, "value".as_bytes().to_vec());
            }
            value => panic!("expected REPLCONF key value, got {:?}", value),
        }
    }

    #[test]
    fn parse_replconf_wrong_args() {
        match from_parts(&["REPLCONF"]) {
            Err(RedisError::Protocol(message)) => assert!(message.starts_with("wrong number")),
            value => panic!("expected protocol error, got {:?}", value),
        }
    }

    #[test]
    fn parse_psync_defaults() {
        match from_parts(&["PSYNC", "?", "-1"]) {
            Ok(RedisCommand::Psync {
                master_replid: None,
                master_repl_offset: None,
            }) => {}
            value => panic!("expected PSYNC with defaults, got {:?}", value),
        }
    }

    #[test]
    fn parse_psync_with_options() {
        match from_parts(&["PSYNC", "id", "0"]) {
            Ok(RedisCommand::Psync {
                master_replid: Some(id),
                master_repl_offset: Some(0),
            }) => assert_eq!(id, "id".as_bytes().to_vec()),
            value => panic!("expected PSYNC id 0, got {:?}", value),
        }
    }

    #[test]
    fn parse_psync_invalid_offset() {
        match from_parts(&["PSYNC", "?", "abc"]) {
            Err(RedisError::Protocol(message)) => {
                assert!(message.starts_with("invalid PSYNC offset"))
            }
            value => panic!("expected protocol error, got {:?}", value),
        }
    }

    #[test]
    fn invalid_command() {
        match from_parts(&["XXX"]) {
            Err(RedisError::Protocol(message)) => assert!(message.starts_with("invalid command")),
            value => panic!("expected protocol error, got {:?}", value),
        }
    }

    fn from_parts(parts: &[&str]) -> Result<RedisCommand, RedisError> {
        let values = parts
            .iter()
            .map(|str| RedisValue::bulk_string(str))
            .collect();

        let array = RedisValue::Array(values);
        from_value(array)
    }

    fn assert_command_value(command: RedisCommand, args: &[&str]) {
        let value = command.to_value();
        let array = args
            .iter()
            .map(|str| RedisValue::bulk_string(str))
            .collect();

        let expected = RedisValue::Array(array);
        assert_eq!(value, expected);
    }
}
