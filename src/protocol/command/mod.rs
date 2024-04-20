use crate::protocol::{RedisError, RedisValue};

mod format;
mod parse;

#[derive(Clone, Debug, PartialEq)]
pub enum RedisCommand {
    Ping {
        message: Option<Vec<u8>>,
    },

    Echo {
        message: Vec<u8>,
    },

    Get {
        key: Vec<u8>,
    },

    Set {
        key: Vec<u8>,
        value: Vec<u8>,
        expiry: Option<u64>,
    },

    Info {
        sections: Vec<Vec<u8>>,
    },

    Replconf {
        key: Vec<u8>,
        value: Vec<u8>,
    },

    Psync {
        master_replid: Option<Vec<u8>>,
        master_repl_offset: Option<u32>,
    },
}

impl RedisCommand {
    pub fn is_write(&self) -> bool {
        matches!(self, Self::Set { .. })
    }

    pub fn to_value(&self) -> RedisValue {
        format::to_value(self)
    }
}

impl TryFrom<RedisValue> for RedisCommand {
    type Error = RedisError;

    fn try_from(value: RedisValue) -> Result<Self, Self::Error> {
        parse::from_value(value)
    }
}
