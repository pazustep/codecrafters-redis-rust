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

    /// The read data can't be correctly interpreted as a RESP value
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
        let prefix = self.read_char().await?;

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

    async fn read_char(&mut self) -> Result<char, ValueReadError> {
        let ch = self
            .reader
            .read_u8()
            .await
            .map_err(|error| match error.kind() {
                io::ErrorKind::UnexpectedEof => ValueReadError::EndOfInput,
                _ => ValueReadError::Io(error),
            })?
            .into();

        Ok(ch)
    }

    pub async fn read_bytes(&mut self) -> Result<(usize, Vec<u8>), ValueReadError> {
        let prefix = self.read_char().await?;

        if prefix != '$' {
            return Err(ValueReadError::Invalid {
                message: "invalid RESP prefix for bulk bytes".to_string(),
                data: vec![prefix as u8],
            });
        }

        let (length_size, length) = self.read_length().await?;

        if length < 0 {
            return Ok((length_size + 1, vec![]));
        }

        let mut data = vec![0u8; length as usize];
        self.reader.read_exact(&mut data).await?;
        Ok((length as usize + length_size + 1, data))
    }

    async fn read_simple_string(&mut self) -> Result<Value, ValueReadError> {
        let (size, value) = self.read_line().await?;
        Ok(Value::SimpleString((size + 1, value)))
    }

    async fn read_simple_error(&mut self) -> Result<Value, ValueReadError> {
        let (size, value) = self.read_line().await?;
        Ok(Value::SimpleError((size + 1, value)))
    }

    async fn read_integer(&mut self) -> Result<Value, ValueReadError> {
        let (size, value) = self.parse_string::<i64>("invalid integer value").await?;
        Ok(Value::Integer((size + 1, value)))
    }

    async fn read_bulk_string(&mut self) -> Result<Value, ValueReadError> {
        let (length_size, length) = self.read_length().await?;
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
        Ok(Value::BulkString((length as usize + length_size + 3, data)))
    }

    async fn read_array(&mut self) -> Result<Value, ValueReadError> {
        let (length_size, length) = self.read_length().await?;

        if length < 0 {
            return Ok(Value::NullArray);
        }

        let mut values = Vec::with_capacity(length as usize);
        let mut values_size = 0;

        for _ in 0..length {
            // pin is required to add indirection to a recursive call
            let value = Box::pin(self.read()).await?;
            values_size += value.size();
            values.push(value);
        }

        Ok(Value::Array((values_size + length_size + 1, values)))
    }

    async fn read_line_bytes(&mut self) -> Result<(usize, Vec<u8>), ValueReadError> {
        let mut line = Vec::new();
        let mut cr_found = false;
        let reader = &mut self.reader;
        let mut total_bytes_read = 0;

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
                return Ok((total_bytes_read + 1, line));
            }

            for i in 0..bytes_read - 1 {
                if bytes[i] == 0xd && bytes[i + 1] == 0xa {
                    line.extend(&bytes[0..i]);
                    reader.consume(i + 2);
                    return Ok((total_bytes_read + i + 2, line));
                }
            }

            if bytes[bytes_read - 1] == 0xd {
                cr_found = true
            }

            // EOL not reached; append data to buffer and loop
            reader.consume(bytes_read);
            line.extend(bytes);
            total_bytes_read += bytes_read;
        }
    }

    async fn read_line(&mut self) -> Result<(usize, String), ValueReadError> {
        let (size, bytes) = self.read_line_bytes().await?;

        let str = String::from_utf8(bytes).map_err(|error| ValueReadError::Invalid {
            message: format!("invalid UTF-8 in simple string: {}", error),
            data: error.into_bytes(),
        })?;

        Ok((size, str))
    }

    async fn parse_string<T>(&mut self, message: &str) -> Result<(usize, T), ValueReadError>
    where
        T: FromStr,
    {
        let (size, value) = self.read_line().await?;
        let parsed = value.parse().map_err(|_| ValueReadError::Invalid {
            message: format!("{}: {}", message, value),
            data: value.into_bytes(),
        })?;

        Ok((size, parsed))
    }

    async fn read_length(&mut self) -> Result<(usize, i32), ValueReadError> {
        self.parse_string("invalid length").await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::BufReader;

    #[tokio::test]
    async fn read_bytes_one_line() {
        let (size, line) = read_line_bytes("OK\r\n").await.unwrap();
        assert_eq!(size, 4);
        assert_eq!(line, "OK".as_bytes());
    }

    #[tokio::test]
    async fn read_bytes_two_lines() {
        let reader = BufReader::new("ONE\r\nTWO\r\n".as_bytes());
        let mut parser = ValueReader::new(reader);

        let (size, one) = parser.read_line_bytes().await.unwrap();
        assert_eq!(size, 5);
        assert_eq!(one, "ONE".as_bytes());

        let (size, two) = parser.read_line_bytes().await.unwrap();
        assert_eq!(size, 5);
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

        let (size, line) = parser.read_line_bytes().await.unwrap();
        assert_eq!(size, 4);
        assert_eq!(line, "OK".as_bytes());

        let (size, line) = parser.read_line_bytes().await.unwrap();
        assert_eq!(size, 7);
        assert_eq!(line, "AGAIN".as_bytes());
    }

    async fn read_line_bytes(buffer: &str) -> Result<(usize, Vec<u8>), ValueReadError> {
        let reader = BufReader::new(buffer.as_bytes());
        let mut parser = ValueReader::new(reader);
        parser.read_line_bytes().await
    }

    #[tokio::test]
    async fn read_simple_string() {
        match read_value("+OK\r\n").await {
            Ok(Value::SimpleString((size, val))) => {
                assert_eq!(size, 5);
                assert_eq!(val, "OK");
            }
            val => panic!("expected simple string, got {:?}", val),
        }
    }

    #[tokio::test]
    async fn read_simple_error() {
        match read_value("-ERR message\r\n").await {
            Ok(Value::SimpleError((size, val))) => {
                assert_eq!(size, 14);
                assert_eq!(val, "ERR message");
            }
            val => panic!("expected simple error, got {:?}", val),
        }
    }

    #[tokio::test]
    async fn read_integer_valid() {
        match read_value(":1\r\n").await {
            Ok(Value::Integer((4, 1))) => {}
            val => panic!("expected Integer(1), got {:?}", val),
        }
    }

    #[tokio::test]
    async fn read_integer_positive_sign() {
        match read_value(":+1\r\n").await {
            Ok(Value::Integer((5, 1))) => {}
            val => panic!("expected Integer(1), got {:?}", val),
        }
    }

    #[tokio::test]
    async fn read_integer_negative() {
        match read_value(":-1\r\n").await {
            Ok(Value::Integer((5, -1))) => {}
            val => panic!("expected Integer(-1), got {:?}", val),
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
            Ok(Value::BulkString((size, val))) => {
                assert_eq!(size, 8);
                assert_eq!(val, "OK".as_bytes())
            }
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
            Ok(Value::Array((size, values))) => {
                assert_eq!(size, 12);
                match values.as_slice() {
                    [Value::BulkString((size, bytes))] => {
                        assert_eq!(*size, 8);
                        assert_eq!(bytes, "OK".as_bytes())
                    }
                    val => panic!("expected OK, got {:?}", val),
                }
            }
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
