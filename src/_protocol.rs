use anyhow::Context;
use std::fmt::{Display, Formatter};
use std::io::{self, BufRead, Write};
use std::{collections::VecDeque, vec};

#[derive(Clone)]
pub enum RedisCommand {
    Ping {
        message: Option<String>,
    },

    Echo {
        message: String,
    },

    Get {
        key: String,
    },

    Set {
        key: String,
        value: String,
        expiry: Option<u64>,
    },

    Info {
        sections: Vec<String>,
    },

    Replconf {
        key: String,
        value: String,
    },

    Psync {
        master_replid: Option<String>,
        master_repl_offset: Option<u32>,
    },
}

impl RedisCommand {
    pub fn is_replica(&self) -> bool {
        matches!(
            self,
            Self::Info { .. } | Self::Replconf { .. } | Self::Psync { .. }
        )
    }

    pub fn is_write(&self) -> bool {
        matches!(self, Self::Set { .. })
    }

    pub fn to_value(&self) -> RedisValue {
        match self {
            Self::Ping { message } => match message {
                Some(message) => RedisValue::command("PING", &[message]),
                None => RedisValue::Array(vec![RedisValue::bulk_string("PING")]),
            },
            Self::Echo { message } => RedisValue::command("ECHO", &[message]),
            Self::Get { key } => RedisValue::command("GET", &[key]),
            Self::Set {
                key,
                value,
                expiry: None,
            } => RedisValue::command("SET", &[key, value]),
            Self::Set {
                key,
                value,
                expiry: Some(expiry),
            } => RedisValue::command("SET", &[key, value, "PX", &expiry.to_string()]),
            Self::Info { sections } => RedisValue::command(
                "INFO",
                &sections.iter().map(|x| x.as_ref()).collect::<Vec<_>>(),
            ),
            Self::Replconf { key, value } => RedisValue::command("REPLCONF", &[key, value]),
            Self::Psync {
                master_replid,
                master_repl_offset,
            } => {
                let replid = match master_replid {
                    Some(master_replid) => master_replid,
                    None => "?",
                };

                let offset = match master_repl_offset {
                    Some(offset) => offset.to_string(),
                    None => "-1".to_string(),
                };

                RedisValue::command("PSYNC", &[replid, &offset])
            }
        }
    }
}

pub fn read_command<R: BufRead>(reader: &mut R) -> Result<RedisCommand, RedisError> {
    match read_value(reader)? {
        RedisValue::Array(command) => parse_command_from_values(command),
        value => {
            let message = format!("unexpected redis value reading command: {}", value);
            Err(RedisError::Protocol(message))
        }
    }
}

fn parse_command_from_values(values: Vec<RedisValue>) -> Result<RedisCommand, RedisError> {
    let mut output = VecDeque::with_capacity(values.len());

    for (idx, value) in values.into_iter().enumerate() {
        match value {
            RedisValue::BulkString(str) => output.push_back(str),
            val => {
                let message = format!("expected bulk string in array index {}, got {}", idx, val);
                return Err(RedisError::CommandInvalid(message));
            }
        }
    }

    parse_command_from_strings(output)
}

fn parse_command_from_strings(mut array: VecDeque<String>) -> Result<RedisCommand, RedisError> {
    let command = array.pop_front().unwrap();

    match command.to_uppercase().as_str() {
        "PING" => Ok(RedisCommand::Ping {
            message: array.pop_front(),
        }),
        "ECHO" => {
            let message = array
                .pop_front()
                .ok_or_else(|| RedisError::wrong_number_of_arguments("echo"))?;
            Ok(RedisCommand::Echo { message })
        }
        "GET" => {
            let key = array
                .pop_front()
                .ok_or_else(|| RedisError::wrong_number_of_arguments("get"))?;

            Ok(RedisCommand::Get { key })
        }
        "SET" => {
            if array.len() < 2 {
                return Err(RedisError::wrong_number_of_arguments("set"));
            }

            let key = array.pop_front().unwrap();
            let value = array.pop_front().unwrap();
            let option = array.pop_front().map(|s| s.to_uppercase());

            let expiry = match option.as_deref() {
                Some("PX") => {
                    let px_value = array
                        .pop_front()
                        .ok_or_else(|| RedisError::wrong_number_of_arguments("set"))?;

                    let px = px_value.parse::<u64>().map_err(|_| {
                        RedisError::CommandInvalid(format!("invalid PX value: {}", px_value))
                    })?;

                    Some(px)
                }
                Some(option) => {
                    let message = format!("unhandled SET option: {}", option);
                    return Err(RedisError::CommandInvalid(message));
                }
                _ => None,
            };

            Ok(RedisCommand::Set { key, value, expiry })
        }
        "INFO" => {
            let sections = array.into();
            Ok(RedisCommand::Info { sections })
        }
        "REPLCONF" => {
            if array.len() < 2 {
                return Err(RedisError::wrong_number_of_arguments("replconf"));
            }

            let key = array.pop_front().unwrap();
            let value = array.pop_front().unwrap();
            Ok(RedisCommand::Replconf { key, value })
        }
        "PSYNC" => {
            if array.len() < 2 {
                return Err(RedisError::wrong_number_of_arguments("psync"));
            }

            let master_replid = array.pop_front().unwrap();
            let master_replid = if master_replid == "?" {
                None
            } else {
                Some(master_replid)
            };

            let master_repl_offset = array.pop_front().unwrap().parse::<i32>();
            let master_repl_offset = if Ok(-1) == master_repl_offset {
                None
            } else if let Ok(offset) = master_repl_offset {
                Some(offset as u32)
            } else {
                return Err(RedisError::command_invalid("invalid master_repl_offset"));
            };

            Ok(RedisCommand::Psync {
                master_replid,
                master_repl_offset,
            })
        }
        _ => Err(RedisError::command_invalid("invalid command")),
    }
}

pub fn read_value<R: BufRead>(reader: &mut R) -> Result<RedisValue, RedisError> {
    let mut buffer = vec![0; 1];

    reader
        .read_exact(&mut buffer)
        .map_err(|_| RedisError::protocol("can't read first byte of redis value"))?;

    let prefix = buffer[0].into();

    match prefix {
        '+' => read_simple_string(reader),
        '$' => read_bulk_string(reader),
        '*' => read_array(reader),
        ch => Err(RedisError::Protocol(format!("unexpected character {}", ch))),
    }
}

pub fn read_bulk_bytes<R: BufRead>(reader: &mut R) -> Result<RedisValue, RedisError> {
    let mut buffer = vec![0; 1];

    reader
        .read_exact(&mut buffer)
        .map_err(|_| RedisError::protocol("can't read first byte of redis value"))?;

    let prefix: char = buffer[0].into();

    if prefix != '$' {
        return Err(RedisError::Protocol(format!(
            "unexpected character {}",
            prefix
        )));
    }

    let len = read_length(reader)?;
    let mut buffer = vec![0; len as usize];

    reader
        .read_exact(&mut buffer)
        .map_err(|_| RedisError::protocol("can't read bulk bytes value"))?;

    Ok(RedisValue::BulkBytes(buffer))
}

fn read_simple_string<R: BufRead>(reader: &mut R) -> Result<RedisValue, RedisError> {
    let mut buffer = String::new();

    reader
        .read_line(&mut buffer)
        .map_err(|_| RedisError::protocol("can't read simple string"))?;

    Ok(RedisValue::SimpleString(buffer))
}

fn read_bulk_string<R: BufRead>(reader: &mut R) -> Result<RedisValue, RedisError> {
    let len = read_length(reader)?;

    if len <= 0 {
        Ok(RedisValue::NullBulkString)
    } else {
        let len = len as usize;
        let mut buffer = vec![0; len + 2];
        reader
            .read_exact(&mut buffer)
            .context("failed to read bulk string value")?;
        buffer.truncate(len);

        let value = String::from_utf8(buffer).context("bulk string is invalid UTF-8")?;
        Ok(RedisValue::BulkString(value))
    }
}

fn read_array<R: BufRead>(reader: &mut R) -> Result<RedisValue, RedisError> {
    let len = read_length(reader)? as usize;
    let mut result = VecDeque::with_capacity(len);

    for _ in 0..len {
        let value = read_value(reader)?;
        result.push_back(value);
    }

    Ok(RedisValue::Array(result.into()))
}

fn read_length<R: BufRead>(reader: &mut R) -> Result<i32, RedisError> {
    // max redis length is 512MB, which is 9 bytes in ASCII + 2 for \r\n
    let mut buffer = String::with_capacity(11);
    reader
        .read_line(&mut buffer)
        .context("failed to read prefixed length")?;

    let len = buffer[..buffer.len() - 2]
        .parse::<i32>()
        .map_err(|e| RedisError::CommandInvalid(format!("invalid length: {}", e)))?;

    Ok(len)
}

impl RedisValue {
    pub fn ok() -> Self {
        Self::simple_string("OK")
    }

    pub fn simple_string(value: &str) -> Self {
        Self::SimpleString(value.to_string())
    }

    pub fn bulk_string(value: &str) -> Self {
        Self::BulkString(value.to_string())
    }

    pub fn command(command: &str, args: &[&str]) -> Self {
        let mut array = Vec::with_capacity(args.len() + 1);
        array.push(Self::bulk_string(command));
        array.extend(args.iter().map(|arg| Self::bulk_string(arg)));
        Self::Array(array)
    }
}

impl RedisValue {
    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        match self {
            RedisValue::SimpleString(str) => write!(writer, "+{}\r\n", str),
            RedisValue::BulkString(str) => write!(writer, "${}\r\n{}\r\n", str.len(), str),
            RedisValue::BulkBytes(bytes) => {
                write!(writer, "${}\r\n", bytes.len())?;
                writer.write_all(bytes)
            }
            RedisValue::Array(array) => {
                write!(writer, "*{}\r\n", array.len())?;

                for value in array {
                    value.write_to(writer)?;
                }

                Ok(())
            }
            RedisValue::NullBulkString => write!(writer, "$-1\r\n"),
        }?;

        writer.flush()
    }
}

impl Display for RedisValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RedisValue::SimpleString(value) => write!(f, "{}", value),
            RedisValue::BulkString(value) => write!(f, "{}", value),
            RedisValue::BulkBytes(_) => write!(f, "<bytes>"),
            RedisValue::Array(array) => {
                let value = array
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>()
                    .join(" ");

                write!(f, "{}", value)
            }
            RedisValue::NullBulkString => write!(f, "_"),
        }
    }
}
