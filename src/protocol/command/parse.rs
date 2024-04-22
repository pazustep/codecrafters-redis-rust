use crate::protocol::{Command, Value};
use std::{collections::VecDeque, time::Duration};

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct FromValueError(String);

fn wrong_number_of_arguments(command: &str) -> FromValueError {
    FromValueError(format!("wrong number of arguments for command {}", command))
}

pub fn from_value(value: Value) -> Result<Command, FromValueError> {
    match value {
        Value::Array(values) => from_values(values),
        value => {
            let message = format!("unexpected RESP value: {:?}", value);
            Err(FromValueError(format!("value must be a RESP array")))
        }
    }
}

fn from_values(values: Vec<Value>) -> Result<Command, FromValueError> {
    if values.is_empty() {
        return Err(FromValueError(format!("RESP array must not be empty")));
    }

    let mut parts = VecDeque::with_capacity(values.len());

    for (idx, value) in values.into_iter().enumerate() {
        match value {
            Value::BulkString(bytes) => {
                parts.push_back(bytes);
            }
            value => {
                return Err(FromValueError(format!(
                    "RESP array element at index {} must be a bulk string",
                    idx
                )));
            }
        }
    }

    from_parts(parts)
}

fn from_parts(mut values: VecDeque<Vec<u8>>) -> Result<Command, FromValueError> {
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

fn parse_ping(mut args: VecDeque<Vec<u8>>) -> Result<Command, FromValueError> {
    let message = args.pop_front();
    Ok(Command::Ping { message })
}

fn parse_echo(mut args: VecDeque<Vec<u8>>) -> Result<Command, FromValueError> {
    let message = args
        .pop_front()
        .ok_or_else(|| wrong_number_of_arguments("ECHO"))?;
    Ok(Command::Echo { message })
}

fn parse_get(mut args: VecDeque<Vec<u8>>) -> Result<Command, FromValueError> {
    let key = args
        .pop_front()
        .ok_or_else(|| wrong_number_of_arguments("GET"))?;

    Ok(Command::Get { key })
}

fn parse_set(mut args: VecDeque<Vec<u8>>) -> Result<Command, FromValueError> {
    if args.len() < 2 {
        return Err(wrong_number_of_arguments("SET"));
    }

    let key = args.pop_front().unwrap();
    let value = args.pop_front().unwrap();
    let expiry = parse_set_args(args)?;

    Ok(Command::Set { key, value, expiry })
}

fn parse_set_args(mut args: VecDeque<Vec<u8>>) -> Result<Option<Duration>, FromValueError> {
    match args.pop_front() {
        Some(arg) => match from_utf8(arg)?.to_uppercase().as_str() {
            "PX" => parse_set_expiry(args).map(Some),
            arg => Err(FromValueError(format!("invalid SET argument: {}", arg))),
        },
        None => Ok(None),
    }
}

fn parse_set_expiry(mut args: VecDeque<Vec<u8>>) -> Result<Duration, FromValueError> {
    let bytes = args
        .pop_front()
        .ok_or_else(|| wrong_number_of_arguments("SET PX"))?;

    let text = from_utf8(bytes)?;

    let expiry = text
        .parse::<u64>()
        .map_err(|_| FromValueError(format!("invalid integer value for SET PX argument")))?;

    Ok(Duration::from_millis(expiry))
}

fn parse_info(args: VecDeque<Vec<u8>>) -> Result<Command, FromValueError> {
    Ok(Command::Info {
        sections: args.into(),
    })
}

fn parse_replconf(mut args: VecDeque<Vec<u8>>) -> Result<Command, FromValueError> {
    if args.len() < 2 {
        return Err(wrong_number_of_arguments("REPLCONF"));
    }

    let key = args.pop_front().unwrap();
    let value = args.pop_front().unwrap();

    Ok(Command::Replconf { key, value })
}

fn parse_psync(mut args: VecDeque<Vec<u8>>) -> Result<Command, FromValueError> {
    if args.len() < 2 {
        return Err(wrong_number_of_arguments("PSYNC"));
    }

    let master_replid = parse_psync_replid(args.pop_front().unwrap());
    let master_repl_offset = parse_psync_offset(args.pop_front().unwrap())?;

    Ok(Command::Psync {
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

fn parse_psync_offset(offset: Vec<u8>) -> Result<Option<u32>, FromValueError> {
    let offset = from_utf8(offset)?;

    let offset = offset
        .parse::<i64>()
        .map_err(|_| FromValueError(format!("invalid PSYNC offset: {}", offset)))?;

    if offset < 0 {
        Ok(None)
    } else {
        Ok(Some(offset as u32))
    }
}

fn invalid_command(command: &str) -> Result<Command, FromValueError> {
    Err(FromValueError(format!("invalid command: {}", command)))
}

fn from_utf8(bytes: Vec<u8>) -> Result<String, FromValueError> {
    String::from_utf8(bytes).map_err(|_| FromValueError(format!("invalid UTF-8")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_redis_value() {
        match from_value(Value::NullBulkString) {
            Err(FromValueError(message)) => {
                assert!(message.starts_with("unexpected RESP value"))
            }
            value => panic!("expected protocol error, got {:?}", value),
        }
    }

    #[test]
    fn empty_array() {
        match from_value(Value::Array(vec![])) {
            Err(FromValueError(message)) => assert_eq!(message, "empty RESP array"),
            value => panic!("expected protocol error, got {:?}", value),
        }
    }

    #[test]
    fn malformed_array() {
        let value = Value::Array(vec![Value::simple_string("OK")]);
        match from_value(value) {
            Err(FromValueError(message)) => {
                assert!(message.starts_with("unexpected RESP value"))
            }
            value => panic!("expected protocol error, got {:?}", value),
        }
    }

    #[test]
    fn ping_without_message() {
        let command = Command::Ping { message: None };
        assert_command_value(command, &["PING"]);
    }

    #[test]
    fn ping_with_message() {
        let command = Command::Ping {
            message: Some("message".as_bytes().to_vec()),
        };
        assert_command_value(command, &["PING", "message"]);
    }

    #[test]
    fn echo() {
        let command = Command::Echo {
            message: "message".as_bytes().to_vec(),
        };

        assert_command_value(command, &["ECHO", "message"]);
    }

    #[test]
    fn get() {
        let command = Command::Get {
            key: "key".as_bytes().to_vec(),
        };

        assert_command_value(command, &["GET", "key"]);
    }

    #[test]
    fn set_without_expiry() {
        let command = Command::Set {
            key: "key".as_bytes().to_vec(),
            value: "value".as_bytes().to_vec(),
            expiry: None,
        };

        assert_command_value(command, &["SET", "key", "value"]);
    }

    #[test]
    fn set_with_expiry() {
        let command = Command::Set {
            key: "key".as_bytes().to_vec(),
            value: "value".as_bytes().to_vec(),
            expiry: Some(Duration::from_millis(1000)),
        };

        assert_command_value(command, &["SET", "key", "value", "PX", "1000"]);
    }

    #[test]
    fn info() {
        let command = Command::Info {
            sections: vec![
                "section1".as_bytes().to_vec(),
                "section2".as_bytes().to_vec(),
            ],
        };

        assert_command_value(command, &["INFO", "section1", "section2"]);
    }

    #[test]
    fn replconf() {
        let command = Command::Replconf {
            key: "key".as_bytes().to_vec(),
            value: "value".as_bytes().to_vec(),
        };

        assert_command_value(command, &["REPLCONF", "key", "value"]);
    }

    #[test]
    fn psync_empty() {
        let command = Command::Psync {
            master_replid: None,
            master_repl_offset: None,
        };

        assert_command_value(command, &["PSYNC", "?", "-1"]);
    }

    #[test]
    fn psync_present() {
        let command = Command::Psync {
            master_replid: Some("replid".as_bytes().to_vec()),
            master_repl_offset: Some(100),
        };

        assert_command_value(command, &["PSYNC", "replid", "100"]);
    }

    #[test]
    fn parse_ping_no_message() {
        let command = from_parts(&["PING"]).unwrap();
        assert_eq!(command, Command::Ping { message: None })
    }

    #[test]
    fn parse_ping_with_message() {
        match from_parts(&["PING", "message"]) {
            Ok(Command::Ping {
                message: Some(bytes),
            }) => assert_eq!(bytes, "message".as_bytes().to_vec()),
            value => panic!("expected PING, got {:?}", value),
        }
    }

    #[test]
    fn parse_echo_ok() {
        match from_parts(&["ECHO", "message"]) {
            Ok(Command::Echo { message }) => {
                assert_eq!(message, "message".as_bytes().to_vec())
            }
            value => panic!("expected ECHO message, got {:?}", value),
        }
    }

    #[test]
    fn parse_echo_wrong_args() {
        match from_parts(&["ECHO"]) {
            Err(FromValueError(_)) => {}
            value => panic!("expected protocol error, got {:?}", value),
        }
    }

    #[test]
    fn parse_get_ok() {
        match from_parts(&["GET", "key"]) {
            Ok(Command::Get { key }) => assert_eq!(key, "key".as_bytes().to_vec()),
            value => panic!("expected GET key, got {:?}", value),
        }
    }

    #[test]
    fn parse_get_wrong_args() {
        match from_parts(&["GET"]) {
            Err(FromValueError(_)) => {}
            value => panic!("expected protocol error, got {:?}", value),
        }
    }

    #[test]
    fn parse_set_without_expiry() {
        match from_parts(&["SET", "key", "value"]) {
            Ok(Command::Set {
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
            Ok(Command::Set {
                expiry: Some(duration),
                ..
            }) if duration.as_millis() == 1000 => {}
            value => panic!("expected SET key value PX 1000, got {:?}", value),
        }
    }

    #[test]
    fn parse_set_px_missing_arg() {
        match from_parts(&["SET", "key", "value", "PX"]) {
            Err(FromValueError(message)) => {
                assert!(message.starts_with("wrong number of arguments"))
            }
            value => panic!("expected protocol error, got {:?}", value),
        }
    }

    #[test]
    fn parse_set_px_not_utf8() {
        let value = Value::Array(vec![
            Value::bulk_string("SET"),
            Value::bulk_string("key"),
            Value::bulk_string("value"),
            Value::bulk_string("PX"),
            Value::BulkString(vec![0xC3, 0x28]),
        ]);

        match Command::try_from(value) {
            Err(FromValueError(message)) => assert_eq!(message, "invalid UTF-8"),
            value => panic!("expected invalid UTF-8, got {:?}", value),
        }
    }

    #[test]
    fn parse_set_px_not_numeric() {
        match from_parts(&["SET", "key", "value", "PX", "abc"]) {
            Err(FromValueError(message)) => assert!(message.starts_with("invalid integer")),
            value => panic!("expected protocol error, got {:?}", value),
        }
    }

    #[test]
    fn parse_set_unknown_arg() {
        match from_parts(&["SET", "key", "value", "EX"]) {
            Err(FromValueError(message)) => {
                assert!(message.starts_with("invalid SET argument"))
            }
            value => panic!("expected protocol error, got {:?}", value),
        }
    }

    #[test]
    fn parse_set_wrong_number_of_args() {
        match from_parts(&["SET", "key"]) {
            Err(FromValueError(message)) => {
                assert!(message.starts_with("wrong number of arguments"))
            }
            value => panic!("expected protocol error, got {:?}", value),
        }
    }

    #[test]
    fn parse_info_one_section() {
        match from_parts(&["INFO", "replication"]) {
            Ok(Command::Info { sections }) => {
                assert_eq!(sections, vec!["replication".as_bytes().to_vec()])
            }
            value => panic!("expected INFO replication, got {:?}", value),
        }
    }

    #[test]
    fn parse_info_empty_sections() {
        match from_parts(&["INFO"]) {
            Ok(Command::Info { sections }) => assert!(sections.is_empty()),
            value => panic!("expected INFO, got {:?}", value),
        }
    }

    #[test]
    fn parse_replconf_ok() {
        match from_parts(&["REPLCONF", "key", "value"]) {
            Ok(Command::Replconf { key, value }) => {
                assert_eq!(key, "key".as_bytes().to_vec());
                assert_eq!(value, "value".as_bytes().to_vec());
            }
            value => panic!("expected REPLCONF key value, got {:?}", value),
        }
    }

    #[test]
    fn parse_replconf_wrong_args() {
        match from_parts(&["REPLCONF"]) {
            Err(FromValueError(message)) => assert!(message.starts_with("wrong number")),
            value => panic!("expected protocol error, got {:?}", value),
        }
    }

    #[test]
    fn parse_psync_defaults() {
        match from_parts(&["PSYNC", "?", "-1"]) {
            Ok(Command::Psync {
                master_replid: None,
                master_repl_offset: None,
            }) => {}
            value => panic!("expected PSYNC with defaults, got {:?}", value),
        }
    }

    #[test]
    fn parse_psync_with_options() {
        match from_parts(&["PSYNC", "id", "0"]) {
            Ok(Command::Psync {
                master_replid: Some(id),
                master_repl_offset: Some(0),
            }) => assert_eq!(id, "id".as_bytes().to_vec()),
            value => panic!("expected PSYNC id 0, got {:?}", value),
        }
    }

    #[test]
    fn parse_psync_invalid_offset() {
        match from_parts(&["PSYNC", "?", "abc"]) {
            Err(FromValueError(message)) => {
                assert!(message.starts_with("invalid PSYNC offset"))
            }
            value => panic!("expected protocol error, got {:?}", value),
        }
    }

    #[test]
    fn invalid_command() {
        match from_parts(&["XXX"]) {
            Err(FromValueError(message)) => assert!(message.starts_with("invalid command")),
            value => panic!("expected protocol error, got {:?}", value),
        }
    }

    fn from_parts(parts: &[&str]) -> Result<Command, FromValueError> {
        let values = parts.iter().map(|str| Value::bulk_string(str)).collect();

        let array = Value::Array(values);
        from_value(array)
    }

    fn assert_command_value(command: Command, args: &[&str]) {
        let value = command.to_value();
        let array = args.iter().map(|str| Value::bulk_string(str)).collect();

        let expected = Value::Array(array);
        assert_eq!(value, expected);
    }
}
