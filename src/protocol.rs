use anyhow::Context;
use std::collections::VecDeque;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[non_exhaustive]
pub enum RedisCommand {
    Noop,

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

pub async fn read_command<R: AsyncBufRead + Unpin>(
    reader: &mut R,
) -> Result<RedisCommand, RedisError> {
    let mut array = read_array_of_bulk_strings(reader).await?;

    let command = array
        .pop_front()
        .ok_or_else(|| RedisError::command_invalid("empty command array"))?;

    match command.to_uppercase().as_str() {
        "" => Ok(RedisCommand::Noop),
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
        _ => Err(RedisError::command_invalid("invalid command")),
    }
}

async fn read_array_of_bulk_strings<R: AsyncBufRead + Unpin>(
    reader: &mut R,
) -> Result<VecDeque<String>, RedisError> {
    let len = read_prefixed_length("*", reader).await?;
    let mut result = VecDeque::with_capacity(len);

    for _ in 0..len {
        let string = read_bulk_string(reader).await?;
        result.push_back(string);
    }

    Ok(result)
}

async fn read_bulk_string<R: AsyncBufRead + Unpin>(reader: &mut R) -> Result<String, RedisError> {
    let len = read_prefixed_length("$", reader).await?;

    if len == 0 {
        return Ok("".to_string());
    }

    let mut buffer = vec![0u8; len + 2];
    reader
        .read_exact(&mut buffer)
        .await
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
    let value = String::from_utf8(buffer).context("bulk string value isn't valid UTF-8")?;

    Ok(value)
}

/// Consumes a line from the reader and parses a length with an arbitrary
/// one-byte prefix.
///
/// This is a helper method to parse other redis protocol elements that use
/// length prefixes, like arrays and strings.
async fn read_prefixed_length<R: AsyncBufRead + Unpin>(
    prefix: &str,
    reader: &mut R,
) -> Result<usize, RedisError> {
    // max redis length is 512MB, which is 9 bytes in ASCII + 2 for \r\n, +1 for the prefix
    let mut buffer = String::with_capacity(12);
    reader
        .read_line(&mut buffer)
        .await
        .context("failed to read prefixed length")?;

    if buffer.is_empty() {
        return Ok(0);
    }

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

pub async fn write_value<W: AsyncWrite + Unpin>(
    writer: &mut W,
    response: &RedisValue,
) -> std::io::Result<()> {
    let buffer = match response {
        RedisValue::SimpleString(value) => {
            format!("+{}\r\n", value)
        }
        RedisValue::BulkString(value) => {
            format!("${}\r\n{}\r\n", value.len(), value)
        }
        RedisValue::Nil => "$-1\r\n".to_string(),
    };

    writer.write_all(buffer.as_bytes()).await
}
