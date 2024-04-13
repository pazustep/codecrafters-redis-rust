use crate::protocol::RedisValue;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ping_witout_message() {
        let command = RedisCommand::Ping { message: None };
        assert_command_value(command, &["PING"]);
    }

    #[test]
    fn ping_with_message() {
        let command = RedisCommand::Ping {
            message: Some("message".to_string()),
        };
        assert_command_value(command, &["PING", "message"]);
    }

    #[test]
    fn echo() {
        let command = RedisCommand::Echo {
            message: "message".to_string(),
        };

        assert_command_value(command, &["ECHO", "message"]);
    }

    #[test]
    fn get() {
        let command = RedisCommand::Get {
            key: "key".to_string(),
        };

        assert_command_value(command, &["GET", "key"]);
    }

    #[test]
    fn set_without_expiry() {
        let command = RedisCommand::Set {
            key: "key".to_string(),
            value: "value".to_string(),
            expiry: None,
        };

        assert_command_value(command, &["SET", "key", "value"]);
    }

    #[test]
    fn set_with_expiry() {
        let command = RedisCommand::Set {
            key: "key".to_string(),
            value: "value".to_string(),
            expiry: Some(1000),
        };

        assert_command_value(command, &["SET", "key", "value", "PX", "1000"]);
    }

    #[test]
    fn info() {
        let command = RedisCommand::Info {
            sections: vec!["section1".to_string(), "section2".to_string()],
        };

        assert_command_value(command, &["INFO", "section1", "section2"]);
    }

    #[test]
    fn replconf() {
        let command = RedisCommand::Replconf {
            key: "key".to_string(),
            value: "value".to_string(),
        };

        assert_command_value(command, &["REPLCONF", "key", "value"]);
    }

    #[test]
    fn psync_empty() {
        let command = RedisCommand::Psync {
            master_replid: None,
            master_repl_offset: None,
        };

        assert_command_value(command, &["PSYNC", "?", "-1"]);
    }

    #[test]
    fn psync_present() {
        let command = RedisCommand::Psync {
            master_replid: Some("replid".to_string()),
            master_repl_offset: Some(100),
        };

        assert_command_value(command, &["PSYNC", "replid", "100"]);
    }

    fn assert_command_value(command: RedisCommand, args: &[&str]) {
        let value = command.to_value();
        let expected = RedisValue::command(args[0], &args[1..]);
        assert_eq!(value, expected);
    }
}
