mod format;
mod parse;
mod reader;

use crate::protocol::Value;
use std::time::Duration;

pub use parse::FromValueError;
pub use reader::{CommandReadError, CommandReader};

#[derive(Clone, Debug, PartialEq)]
pub enum Command {
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
        expiry: Option<Duration>,
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

impl Command {
    pub fn is_write(&self) -> bool {
        matches!(self, Self::Set { .. })
    }

    pub fn to_value(&self) -> Value {
        format::to_value(self)
    }
}

impl TryFrom<Value> for Command {
    type Error = FromValueError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        parse::from_value(value)
    }
}
