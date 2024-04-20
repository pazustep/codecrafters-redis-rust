use crate::protocol::RedisError;
use crate::protocol::RedisValue;
use anyhow::Context;
use std::str::FromStr;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, BufReader};

pub struct RedisValueParser<R> {
    reader: BufReader<R>,
}

impl<R> RedisValueParser<R>
where
    R: AsyncRead + Unpin,
{
    pub fn new(reader: BufReader<R>) -> Self {
        Self { reader }
    }

    pub async fn read_value(&mut self) -> Result<RedisValue, RedisError> {
        let prefix = self.reader.read_u8().await?.into();

        match prefix {
            '+' => self.read_simple_string().await,
            '-' => self.read_simple_error().await,
            ':' => self.read_integer().await,
            '$' => self.read_bulk_string().await,
            '*' => self.read_array().await,
            _ => {
                let message = format!("invalid RESP value: {}", prefix);
                Err(RedisError::Protocol(message))
            }
        }
    }

    pub async fn read_bytes(&mut self) -> Result<Vec<u8>, RedisError> {
        let length = self.read_length().await?;

        if length < 0 {
            return Ok(vec![]);
        }

        let mut data = vec![0u8; length as usize];
        self.reader.read_exact(&mut data).await?;
        Ok(data)
    }

    async fn read_simple_string(&mut self) -> Result<RedisValue, RedisError> {
        let value = self.read_line().await?;
        Ok(RedisValue::SimpleString(value))
    }

    async fn read_simple_error(&mut self) -> Result<RedisValue, RedisError> {
        let value = self.read_line().await?;
        Ok(RedisValue::SimpleError(value))
    }

    async fn read_integer(&mut self) -> Result<RedisValue, RedisError> {
        let value = self.parse_string::<i64>("invalid integer value").await?;
        Ok(RedisValue::Integer(value))
    }

    async fn read_bulk_string(&mut self) -> Result<RedisValue, RedisError> {
        let length = self.read_length().await?;
        if length < 0 {
            return Ok(RedisValue::NullBulkString);
        }

        let mut data = vec![0u8; length as usize + 2];

        self.reader.read_exact(&mut data).await?;

        if data[data.len() - 2..] != [0xd, 0xa] {
            return Err(RedisError::protocol("bulk string not terminated by \\r\\n"));
        }

        data.resize(data.len() - 2, 0);
        Ok(RedisValue::BulkString(data))
    }

    async fn read_array(&mut self) -> Result<RedisValue, RedisError> {
        let length = self.read_length().await?;

        if length < 0 {
            return Ok(RedisValue::NullArray);
        }

        let mut values = Vec::with_capacity(length as usize);

        for _ in 0..length {
            // pin is required to add indirection to a recursive call
            let value = Box::pin(self.read_value()).await?;
            values.push(value);
        }

        Ok(RedisValue::Array(values))
    }

    async fn read_line_bytes(&mut self) -> Result<Vec<u8>, RedisError> {
        let mut line = Vec::new();
        let mut cr_found = false;
        let reader = &mut self.reader;

        loop {
            let bytes = reader.fill_buf().await?.to_vec();
            let bytes_read = bytes.len();

            if bytes_read == 0 {
                return Err(RedisError::EndOfInput);
            }

            // edge case: CR and LF across two reads
            if cr_found && bytes[0] == 0xa {
                line.pop();
                reader.consume(1);
                return Ok(line);
            }

            for i in 0..bytes_read - 1 {
                if bytes[i] == 0xd && bytes[i + 1] == 0xa {
                    line.extend(&bytes[0..i]);
                    reader.consume(i + 2);
                    return Ok(line);
                }
            }

            if bytes[bytes_read - 1] == 0xd {
                cr_found = true
            }

            // EOL not reached; append data to buffer and loop
            reader.consume(bytes_read);
            line.extend(bytes);
        }
    }

    async fn read_line(&mut self) -> Result<String, RedisError> {
        let bytes = self.read_line_bytes().await?;
        let value = String::from_utf8(bytes).context("invalid UTF-8 in simple string")?;
        Ok(value)
    }

    async fn parse_string<T>(&mut self, error: &str) -> Result<T, RedisError>
    where
        T: FromStr,
    {
        let value = self.read_line().await?;

        let value: T = value.parse().map_err(|_| {
            let message = format!("{}: {}", error, value);
            RedisError::Protocol(message)
        })?;

        Ok(value)
    }

    async fn read_length(&mut self) -> Result<i32, RedisError> {
        self.parse_string("invalid length").await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn read_bytes_one_line() {
        let line = read_line_bytes("OK\r\n").await.unwrap();
        assert_eq!(line, "OK".as_bytes());
    }

    #[tokio::test]
    async fn read_bytes_two_lines() {
        let reader = BufReader::new("ONE\r\nTWO\r\n".as_bytes());
        let mut parser = RedisValueParser::new(reader);

        let one = parser.read_line_bytes().await.unwrap();
        assert_eq!(one, "ONE".as_bytes());

        let two = parser.read_line_bytes().await.unwrap();
        assert_eq!(two, "TWO".as_bytes());
    }

    #[tokio::test]
    async fn read_bytes_eof() {
        let result = read_line_bytes("OK").await;
        match result {
            Err(RedisError::EndOfInput) => { /* ok */ }
            value => panic!("expected end of input, got {:?}", value),
        }
    }

    #[tokio::test]
    async fn read_bytes_crlf_in_boundary() {
        let reader = AsyncReadExt::chain("OK\r".as_bytes(), "\nAGAIN\r\n".as_bytes());
        let reader = BufReader::new(reader);
        let mut parser = RedisValueParser::new(reader);

        let line = parser.read_line_bytes().await.unwrap();
        assert_eq!(line, "OK".as_bytes());

        let line = parser.read_line_bytes().await.unwrap();
        assert_eq!(line, "AGAIN".as_bytes());
    }

    async fn read_line_bytes(buffer: &str) -> Result<Vec<u8>, RedisError> {
        let reader = BufReader::new(buffer.as_bytes());
        let mut parser = RedisValueParser::new(reader);
        parser.read_line_bytes().await
    }

    #[tokio::test]
    async fn read_simple_string() {
        match read_value("+OK\r\n").await {
            Ok(RedisValue::SimpleString(val)) => assert_eq!(val, "OK"),
            val => panic!("expected simple string, got {:?}", val),
        }
    }

    #[tokio::test]
    async fn read_simple_error() {
        match read_value("-ERR message\r\n").await {
            Ok(RedisValue::SimpleError(val)) => assert_eq!(val, "ERR message"),
            val => panic!("expected simple error, got {:?}", val),
        }
    }

    #[tokio::test]
    async fn read_integer_valid() {
        match read_value(":1\r\n").await {
            Ok(RedisValue::Integer(1)) => {}
            val => panic!("expected Integer(1), got {:?}", val),
        }
    }
    #[tokio::test]
    async fn read_integer_invalid() {
        match read_value(":x\r\n").await {
            Err(RedisError::Protocol(_)) => {}
            val => panic!("expected protocol error, got {:?}", val),
        }
    }

    #[tokio::test]
    async fn read_bulk_string_valid() {
        match read_value("$2\r\nOK\r\n").await {
            Ok(RedisValue::BulkString(val)) => assert_eq!(val, "OK".as_bytes()),
            val => panic!("expected BulkString(OK), got {:?}", val),
        }
    }

    #[tokio::test]
    async fn read_bulk_string_invalid() {
        match read_value("$2\r\nOKxx").await {
            Err(RedisError::Protocol(_)) => {}
            val => panic!("expected protocol error, got {:?}", val),
        }
    }

    #[tokio::test]
    async fn read_array_valid() {
        match read_value("*1\r\n$2\r\nOK\r\n").await {
            Ok(RedisValue::Array(values)) => match values.as_slice() {
                [RedisValue::BulkString(bytes)] => assert_eq!(bytes, "OK".as_bytes()),
                val => panic!("expected OK, got {:?}", val),
            },
            val => panic!("expected array, got {:?}", val),
        }
    }

    #[tokio::test]
    async fn read_null_bulk_string() {
        match read_value("$-1\r\n").await {
            Ok(RedisValue::NullBulkString) => {}
            val => panic!("expected NullBulkString, got {:?}", val),
        }
    }

    #[tokio::test]
    async fn read_null_array() {
        match read_value("*-1\r\n").await {
            Ok(RedisValue::NullArray) => {}
            val => panic!("expected null array, got {:?}", val),
        }
    }

    #[tokio::test]
    async fn read_empty() {
        match read_value("").await {
            Err(RedisError::EndOfInput) => {}
            val => panic!("expected end of input, got {:?}", val),
        }
    }

    #[tokio::test]
    async fn read_invalid() {
        match read_value("x").await {
            Err(RedisError::Protocol(msg)) => assert_eq!(msg, "invalid RESP value: x"),
            val => panic!("expected protocol error, got {:?}", val),
        }
    }

    async fn read_value(buffer: &str) -> Result<RedisValue, RedisError> {
        let bytes = buffer.as_bytes();
        let reader = BufReader::new(bytes);
        let mut parser = RedisValueParser::new(reader);
        parser.read_value().await
    }
}
