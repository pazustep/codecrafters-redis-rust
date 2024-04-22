use crate::protocol::Value;
use std::io;
use tokio::io::{AsyncWrite, AsyncWriteExt};

pub struct ValueWriter<W> {
    writer: W,
}

impl<W> ValueWriter<W>
where
    W: AsyncWrite + Unpin,
{
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    pub async fn write(&mut self, value: &Value) -> io::Result<()> {
        match value {
            Value::SimpleString(val) => self.write_simple_string(val).await,
            Value::SimpleError(val) => self.write_simple_error(val).await,
            Value::Integer(val) => self.write_integer(*val).await,
            Value::BulkString(bytes) => self.write_bulk_string(bytes.as_slice()).await,
            Value::Array(values) => self.write_array(values.as_slice()).await,
            Value::NullBulkString => self.write_null_bulk_string().await,
            Value::NullArray => self.write_null_array().await,
        }?;

        self.writer.flush().await
    }

    pub async fn flush(&mut self) -> io::Result<()> {
        self.writer.flush().await
    }

    async fn write_simple_string(&mut self, val: &str) -> io::Result<()> {
        let value = format!("+{}\r\n", val);
        self.writer.write_all(value.as_bytes()).await
    }

    async fn write_simple_error(&mut self, val: &str) -> io::Result<()> {
        let value = format!("-{}\r\n", val);
        self.writer.write_all(value.as_bytes()).await
    }

    async fn write_integer(&mut self, val: i64) -> io::Result<()> {
        let value = format!(":{}\r\n", val);
        self.writer.write_all(value.as_bytes()).await
    }

    async fn write_bulk_string(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.writer.write_all("$".as_bytes()).await?;
        self.writer
            .write_all(bytes.len().to_string().as_bytes())
            .await?;
        self.writer.write_all("\r\n".as_bytes()).await?;
        self.writer.write_all(bytes).await?;
        self.writer.write_all("\r\n".as_bytes()).await
    }

    async fn write_array(&mut self, values: &[Value]) -> io::Result<()> {
        self.writer.write_all("*".as_bytes()).await?;
        self.writer
            .write_all(values.len().to_string().as_bytes())
            .await?;
        self.writer.write_all("\r\n".as_bytes()).await?;

        for value in values {
            Box::pin(self.write(value)).await?;
        }

        Ok(())
    }

    async fn write_null_bulk_string(&mut self) -> io::Result<()> {
        self.writer.write_all("$-1\r\n".as_bytes()).await
    }

    async fn write_null_array(&mut self) -> io::Result<()> {
        self.writer.write_all("*-1\r\n".as_bytes()).await
    }
}
