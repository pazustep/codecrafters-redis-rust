use std::{
    collections::VecDeque,
    io::{BufRead, Write},
};

use anyhow::Context;

#[non_exhaustive]
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
}

#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum RedisError {
    #[error("{0}")]
    CommandInvalid(String),

    #[error(transparent)]
    Unexpected(#[from] anyhow::Error),
}

impl RedisError {
    fn command_invalid(message: &str) -> Self {
        Self::CommandInvalid(message.to_string())
    }

    fn wrong_number_of_arguments(command: &str) -> Self {
        let message = format!("wrong number of arguments for '{}' command", command);
        RedisError::CommandInvalid(message)
    }
}

pub enum RedisValue {
    SimpleString(String),

    BulkString(String),

    Nil,
}

pub fn read_command<R: BufRead>(reader: &mut R) -> Result<RedisCommand, RedisError> {
    let mut array = read_array_of_bulk_strings(reader)?;

    let command = array
        .pop_front()
        .ok_or_else(|| RedisError::command_invalid("empty command array"))?;

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
        _ => Err(RedisError::command_invalid("invalid command")),
    }
}

fn read_array_of_bulk_strings<R: BufRead>(reader: &mut R) -> Result<VecDeque<String>, RedisError> {
    let len = read_prefixed_length("*", reader)?;
    let mut result = VecDeque::with_capacity(len);

    for _ in 0..len {
        let string = read_bulk_string(reader)?;
        result.push_back(string);
    }

    Ok(result)
}

fn read_bulk_string<R: BufRead>(reader: &mut R) -> Result<String, RedisError> {
    let len = read_prefixed_length("$", reader)?;

    let mut buffer = vec![0u8; len + 2];
    reader
        .read_exact(&mut buffer)
        .context("failed to read bulk string")?;

    if buffer.len() != len + 2 {
        let message = format!(
            "failed to read bulk string; expected {} bytes but got {}",
            len + 2,
            buffer.len()
        );
        return Err(RedisError::CommandInvalid(message));
    }

    buffer.truncate(len);
    let value = String::from_utf8(buffer).context("bulk string value isn't value UTF-8")?;

    Ok(value)
}

/// Consumes a line from the reader and parses a length with an arbitrary
/// one-byte prefix.
///
/// This is a helper method to parse other redis protocol elements that use
/// length prefixes, like arrays and strings.
fn read_prefixed_length<R: BufRead>(prefix: &str, reader: &mut R) -> Result<usize, RedisError> {
    // max redis length is 512MB, which is 9 bytes in ASCII + 2 for \r\n, +1 for the prefix
    let mut buffer = String::with_capacity(12);
    reader
        .read_line(&mut buffer)
        .context("failed to read prefixed length")?;

    let read_prefix = &buffer[0..prefix.len()];

    if prefix != read_prefix {
        let message = format!(
            "failed to read prefixed length; expected prefix {} but got {}",
            prefix, read_prefix
        );
        return Err(RedisError::CommandInvalid(message));
    }

    let len = buffer[prefix.len()..buffer.len() - 2]
        .parse::<usize>()
        .map_err(|e| RedisError::CommandInvalid(format!("invalid length: {}", e)))?;

    Ok(len)
}

pub fn write_value<W: Write>(writer: &mut W, response: &RedisValue) -> std::io::Result<()> {
    match response {
        RedisValue::SimpleString(value) => {
            write!(writer, "+{}\r\n", value)
        }
        RedisValue::BulkString(value) => {
            write!(writer, "${}\r\n{}\r\n", value.len(), value)
        }
        RedisValue::Nil => {
            write!(writer, "$-1\r\n")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufReader, Cursor};

    #[test]
    fn test_read_array_of_strings() {
        let input = Cursor::new("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        let result = read_array_of_bulk_strings(&mut BufReader::new(input)).unwrap();
        assert_eq!(result, vec!["foo".to_string(), "bar".to_string()]);
    }

    #[test]
    fn test_read_bulk_string() {
        let input = Cursor::new("$5\r\nhello\r\n");
        let result = read_bulk_string(&mut BufReader::new(input)).unwrap();
        assert_eq!(result, "hello");
    }

    #[test]
    fn test_read_prefixed_length() {
        let input = Cursor::new("$5\r\n");
        let result = read_prefixed_length("$", &mut BufReader::new(input)).unwrap();
        assert_eq!(result, 5);
    }
}
