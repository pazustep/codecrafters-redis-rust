#[derive(thiserror::Error, Debug)]
pub enum RedisError {
    #[error("{0}")]
    Protocol(String),

    #[error(transparent)]
    Unexpected(#[from] anyhow::Error),
}

impl RedisError {
    pub fn protocol(message: &str) -> Self {
        Self::Protocol(message.to_string())
    }

    pub fn invalid_command(message: &str) -> Self {
        Self::Protocol(message.to_string())
    }

    pub fn wrong_number_of_arguments(command: &str) -> Self {
        let message = format!("wrong number of arguments for '{}' command", command);
        RedisError::Protocol(message)
    }
}
