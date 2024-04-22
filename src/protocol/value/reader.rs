use crate::protocol::Value;
use std::{io, str::FromStr};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt};

/// A possible error reading a RESP value.
///
/// This is the error type for the [`read`] method on [`ValueReader`].
#[derive(Debug, thiserror::Error)]
pub enum ValueReadError {
    /// EOF reached when _starting_ to read a RESP value
    #[error("EOF reached; no value to read")]
    EndOfInput,

    /// The read data can't be correctly intepreted as a RESP value
    #[error("{message}")]
    Invalid { message: String, data: Vec<u8> },

    /// An unexpected I/O error ocurred while reading data
    #[error(transparent)]
    Io(#[from] io::Error),
}

pub struct ValueReader<R> {
    reader: R,
}

impl<R> ValueReader<R>
where
    R: AsyncBufRead + Unpin,
{
    pub fn new(reader: R) -> Self {
        Self { reader }
    }

    pub async fn read(&mut self) -> Result<Value, ValueReadError> {
        let prefix = self
            .reader
            .read_u8()
            .await
            .map_err(|error| match error.kind() {
                io::ErrorKind::UnexpectedEof => ValueReadError::EndOfInput,
                _ => ValueReadError::Io(error),
            })?
            .into();

        match prefix {
            '+' => self.read_simple_string().await,
            '-' => self.read_simple_error().await,
            ':' => self.read_integer().await,
            '$' => self.read_bulk_string().await,
            '*' => self.read_array().await,
            _ => Err(ValueReadError::Invalid {
                message: format!("invalid RESP value: {}", prefix),
                data: {
                    let mut buffer = [0; 4];
                    prefix.encode_utf8(&mut buffer).as_bytes().to_vec()
                },
            }),
        }
    }

    pub async fn read_bytes(&mut self) -> Result<Vec<u8>, ValueReadError> {
        let length = self.read_length().await?;

        if length < 0 {
            return Ok(vec![]);
        }

        let mut data = vec![0u8; length as usize];
        self.reader.read_exact(&mut data).await?;
        Ok(data)
    }

    async fn read_simple_string(&mut self) -> Result<Value, ValueReadError> {
        let value = self.read_line().await?;
        Ok(Value::SimpleString(value))
    }

    async fn read_simple_error(&mut self) -> Result<Value, ValueReadError> {
        let value = self.read_line().await?;
        Ok(Value::SimpleError(value))
    }

    async fn read_integer(&mut self) -> Result<Value, ValueReadError> {
        let value = self.parse_string::<i64>("invalid integer value").await?;
        Ok(Value::Integer(value))
    }

    async fn read_bulk_string(&mut self) -> Result<Value, ValueReadError> {
        let length = self.read_length().await?;
        if length < 0 {
            return Ok(Value::NullBulkString);
        }

        let mut data = vec![0u8; length as usize + 2];

        self.reader.read_exact(&mut data).await?;

        if data[data.len() - 2..] != [0xd, 0xa] {
            return Err(ValueReadError::Invalid {
                message: "bulk string not terminated by \\r\\n".to_string(),
                data,
            });
        }

        data.resize(data.len() - 2, 0);
        Ok(Value::BulkString(data))
    }

    async fn read_array(&mut self) -> Result<Value, ValueReadError> {
        let length = self.read_length().await?;

        if length < 0 {
            return Ok(Value::NullArray);
        }

        let mut values = Vec::with_capacity(length as usize);

        for _ in 0..length {
            // pin is required to add indirection to a recursive call
            let value = Box::pin(self.read()).await?;
            values.push(value);
        }

        Ok(Value::Array(values))
    }

    async fn read_line_bytes(&mut self) -> Result<Vec<u8>, ValueReadError> {
        let mut line = Vec::new();
        let mut cr_found = false;
        let reader = &mut self.reader;

        loop {
            let bytes = reader.fill_buf().await?.to_vec();
            let bytes_read = bytes.len();

            if bytes_read == 0 {
                return Err(ValueReadError::Io(io::ErrorKind::UnexpectedEof.into()));
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

    async fn read_line(&mut self) -> Result<String, ValueReadError> {
        let bytes = self.read_line_bytes().await?;

        String::from_utf8(bytes).map_err(|error| ValueReadError::Invalid {
            message: format!("invalid UTF-8 in simple string: {}", error),
            data: error.into_bytes(),
        })
    }

    async fn parse_string<T>(&mut self, message: &str) -> Result<T, ValueReadError>
    where
        T: FromStr,
    {
        let value = self.read_line().await?;
        value.parse().map_err(|_| ValueReadError::Invalid {
            message: format!("{}: {}", message, value),
            data: value.into_bytes(),
        })
    }

    async fn read_length(&mut self) -> Result<i32, ValueReadError> {
        self.parse_string("invalid length").await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::BufReader;

    #[tokio::test]
    async fn read_bytes_one_line() {
        let line = read_line_bytes("OK\r\n").await.unwrap();
        assert_eq!(line, "OK".as_bytes());
    }

    #[tokio::test]
    async fn read_bytes_two_lines() {
        let reader = BufReader::new("ONE\r\nTWO\r\n".as_bytes());
        let mut parser = ValueReader::new(reader);

        let one = parser.read_line_bytes().await.unwrap();
        assert_eq!(one, "ONE".as_bytes());

        let two = parser.read_line_bytes().await.unwrap();
        assert_eq!(two, "TWO".as_bytes());
    }

    #[tokio::test]
    async fn read_bytes_eof() {
        let result = read_line_bytes("OK").await;
        match result {
            Err(ValueReadError::Io(cause)) if cause.kind() == io::ErrorKind::UnexpectedEof => {}
            value => panic!("expected end of input, got {:?}", value),
        }
    }

    #[tokio::test]
    async fn read_bytes_crlf_in_boundary() {
        let reader = AsyncReadExt::chain("OK\r".as_bytes(), "\nAGAIN\r\n".as_bytes());
        let reader = BufReader::new(reader);
        let mut parser = ValueReader::new(reader);

        let line = parser.read_line_bytes().await.unwrap();
        assert_eq!(line, "OK".as_bytes());

        let line = parser.read_line_bytes().await.unwrap();
        assert_eq!(line, "AGAIN".as_bytes());
    }

    async fn read_line_bytes(buffer: &str) -> Result<Vec<u8>, ValueReadError> {
        let reader = BufReader::new(buffer.as_bytes());
        let mut parser = ValueReader::new(reader);
        parser.read_line_bytes().await
    }

    #[tokio::test]
    async fn read_simple_string() {
        match read_value("+OK\r\n").await {
            Ok(Value::SimpleString(val)) => assert_eq!(val, "OK"),
            val => panic!("expected simple string, got {:?}", val),
        }
    }

    #[tokio::test]
    async fn read_simple_error() {
        match read_value("-ERR message\r\n").await {
            Ok(Value::SimpleError(val)) => assert_eq!(val, "ERR message"),
            val => panic!("expected simple error, got {:?}", val),
        }
    }

    #[tokio::test]
    async fn read_integer_valid() {
        match read_value(":1\r\n").await {
            Ok(Value::Integer(1)) => {}
            val => panic!("expected Integer(1), got {:?}", val),
        }
    }
    #[tokio::test]
    async fn read_integer_invalid() {
        match read_value(":x\r\n").await {
            Err(ValueReadError::Invalid { .. }) => {}
            val => panic!("expected protocol error, got {:?}", val),
        }
    }

    #[tokio::test]
    async fn read_bulk_string_valid() {
        match read_value("$2\r\nOK\r\n").await {
            Ok(Value::BulkString(val)) => assert_eq!(val, "OK".as_bytes()),
            val => panic!("expected BulkString(OK), got {:?}", val),
        }
    }

    #[tokio::test]
    async fn read_bulk_string_invalid() {
        match read_value("$2\r\nOKxx").await {
            Err(ValueReadError::Invalid { .. }) => {}
            val => panic!("expected protocol error, got {:?}", val),
        }
    }

    #[tokio::test]
    async fn read_array_valid() {
        match read_value("*1\r\n$2\r\nOK\r\n").await {
            Ok(Value::Array(values)) => match values.as_slice() {
                [Value::BulkString(bytes)] => assert_eq!(bytes, "OK".as_bytes()),
                val => panic!("expected OK, got {:?}", val),
            },
            val => panic!("expected array, got {:?}", val),
        }
    }

    #[tokio::test]
    async fn read_null_bulk_string() {
        match read_value("$-1\r\n").await {
            Ok(Value::NullBulkString) => {}
            val => panic!("expected NullBulkString, got {:?}", val),
        }
    }

    #[tokio::test]
    async fn read_null_array() {
        match read_value("*-1\r\n").await {
            Ok(Value::NullArray) => {}
            val => panic!("expected null array, got {:?}", val),
        }
    }

    #[tokio::test]
    async fn read_empty() {
        match read_value("").await {
            Err(ValueReadError::EndOfInput) => {}
            val => panic!("expected end of input, got {:?}", val),
        }
    }

    #[tokio::test]
    async fn read_invalid() {
        match read_value("x").await {
            Err(ValueReadError::Invalid { message, .. }) => {
                assert_eq!(message, "invalid RESP value: x")
            }
            val => panic!("expected protocol error, got {:?}", val),
        }
    }

    async fn read_value(buffer: &str) -> Result<Value, ValueReadError> {
        let bytes = buffer.as_bytes();
        let reader = BufReader::new(bytes);
        let mut parser = ValueReader::new(reader);
        parser.read().await
    }
}
