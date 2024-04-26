mod reader;
mod writer;

pub use reader::*;
pub use writer::*;

#[derive(Debug, PartialEq, Eq)]
pub enum Value {
    SimpleString(String),

    SimpleError(String),

    Integer(i64),

    BulkString(Vec<u8>),

    BulkBytes(Vec<u8>),

    Array(Vec<Value>),

    NullBulkString,

    NullArray,
}

impl Value {
    pub fn simple_string(value: &str) -> Self {
        Self::SimpleString(value.to_string())
    }

    pub fn simple_error(value: &str) -> Self {
        Self::SimpleError(value.to_string())
    }

    pub fn integer(value: i64) -> Self {
        Self::Integer(value)
    }

    pub fn bulk_string_from_bytes(value: Vec<u8>) -> Self {
        Self::BulkString(value)
    }

    pub fn bulk_string(value: &str) -> Self {
        Self::bulk_string_from_bytes(value.as_bytes().to_vec())
    }

    pub fn command(command: &str, args: &[&Vec<u8>]) -> Self {
        let mut array = Vec::with_capacity(args.len() + 1);
        array.push(Self::bulk_string(command));

        for arg in args {
            array.push(Self::BulkString((*arg).clone()));
        }

        Self::Array(array)
    }

    pub fn command_str(command: &str, args: &[&str]) -> Self {
        let mut array = Vec::with_capacity(args.len() + 1);
        array.push(Self::bulk_string(command));

        for arg in args {
            array.push(Self::bulk_string(arg));
        }

        Self::Array(array)
    }

    pub fn ok() -> Self {
        Self::SimpleString("OK".to_string())
    }
}
