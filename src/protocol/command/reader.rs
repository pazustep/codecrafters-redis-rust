use std::error::Error;

use crate::protocol::{Command, Value, ValueReadError, ValueReader};
use tokio::io::AsyncBufRead;

pub struct CommandReader<R> {
    reader: ValueReader<R>,
}
#[derive(Debug, thiserror::Error)]
pub enum CommandReadError {
    #[error("invalid command")]
    Invalid(Vec<Value>),

    #[error("fatal error reading command")]
    Stop(Option<Box<dyn Error>>),
}

impl<R> CommandReader<R>
where
    R: AsyncBufRead + Unpin,
{
    pub fn new(reader: ValueReader<R>) -> Self {
        Self { reader }
    }

    pub async fn read(&mut self) -> Result<Command, CommandReadError> {
        match self.reader.read().await {
            Ok(value) => parse_command(value),
            Err(ValueReadError::EndOfInput) => Err(CommandReadError::Stop(None)),
            Err(ValueReadError::Invalid { message, .. }) => {
                let message = format!("invalid RESP value: {}", message);
                Err(CommandReadError::Invalid(vec![Value::SimpleError(message)]))
            }
            Err(err) => {
                println!("I/O error reading command: {}", err);
                Err(CommandReadError::Stop(Some(Box::new(err))))
            }
        }
    }
}

fn parse_command(value: Value) -> Result<Command, CommandReadError> {
    match Command::try_from(value) {
        Ok(command) => Ok(command),
        Err(err) => {
            let message = format!("{}", err);
            Err(CommandReadError::Invalid(vec![Value::SimpleError(message)]))
        }
    }
}
