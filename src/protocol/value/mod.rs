mod reader;
mod writer;

pub use reader::*;
pub use writer::*;

#[derive(Debug, PartialEq, Eq)]
pub enum Value {
    SimpleString((usize, String)),

    SimpleError((usize, String)),

    Integer((usize, i64)),

    BulkString((usize, Vec<u8>)),

    BulkBytes((usize, Vec<u8>)),

    Array((usize, Vec<Value>)),

    NullBulkString,

    NullArray,
}

impl Value {
    pub fn simple_string(value: &str) -> Self {
        let len_size = value.len().to_string().len();
        Self::SimpleString((len_size + 3, value.to_string()))
    }

    pub fn simple_error(value: &str) -> Self {
        let len_size = value.len().to_string().len();
        Self::SimpleError((len_size + 3, value.to_string()))
    }

    pub fn bulk_string_from_bytes(value: Vec<u8>) -> Self {
        let len_size = value.len().to_string().len();
        Self::BulkString((len_size + 3, value))
    }

    pub fn bulk_string(value: &str) -> Self {
        Self::bulk_string_from_bytes(value.as_bytes().to_vec())
    }

    pub fn command(command: &str, args: &[&Vec<u8>]) -> Self {
        build_command(command, args.iter().map(|arg| (*arg).clone()))
    }

    pub fn command_str(command: &str, args: &[&str]) -> Self {
        build_command(command, args.iter().map(|arg| arg.as_bytes().to_vec()))
    }

    pub fn integer(value: i64) -> Self {
        let size = value.to_string().len();
        Self::Integer((size, value))
    }

    pub fn ok() -> Self {
        Self::simple_string("OK")
    }

    pub fn size(&self) -> usize {
        match self {
            Value::SimpleString((size, _)) => *size,
            Value::SimpleError((size, _)) => *size,
            Value::Integer((size, _)) => *size,
            Value::BulkString((size, _)) => *size,
            Value::BulkBytes((size, _)) => *size,
            Value::Array((size, _)) => *size,
            Value::NullBulkString => 5,
            Value::NullArray => 5,
        }
    }
}

fn build_command(command: &str, args: impl Iterator<Item = Vec<u8>>) -> Value {
    let command = Value::bulk_string(command);
    let mut array = match args.size_hint() {
        (_, Some(upper)) => Vec::with_capacity(upper + 1),
        _ => Vec::new(),
    };

    let mut parts_size = command.size();
    array.push(command);

    for arg in args {
        let arg = Value::bulk_string_from_bytes(arg);
        parts_size += arg.size();
        array.push(arg);
    }

    let len = format!("{}", array.len());
    Value::Array((len.len() + 3 + parts_size, array))
}
