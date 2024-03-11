use anyhow::{ensure, Context};
use anyhow::{Ok, Result};
use std::io::{BufRead, Write};

pub fn read_array_of_bulk_strings<R: BufRead>(reader: &mut R) -> Result<Vec<String>> {
    let len = read_prefixed_length("*", reader)?;
    let mut result = Vec::with_capacity(len);

    for _ in 0..len {
        let string = read_bulk_string(reader)?;
        result.push(string);
    }

    Ok(result)
}

pub fn read_bulk_string<R: BufRead>(reader: &mut R) -> Result<String> {
    let len = read_prefixed_length("$", reader)?;

    let mut buffer = vec![0u8; len + 2];
    reader
        .read_exact(&mut buffer)
        .context("failed to read bulk string")?;

    ensure!(
        buffer.len() == len + 2,
        "expected to ready {} bytes, but got {}",
        len + 2,
        buffer.len()
    );

    buffer.truncate(len);
    let value = String::from_utf8(buffer)?;

    Ok(value)
}

/// Consumes a line from the reader and parses a length with an arbitrary
/// one-byte prefix.
///
/// This is a helper method to parse other redis protocol elements that use
/// length prefixes, like arrays and strings.
fn read_prefixed_length<R: BufRead>(prefix: &str, reader: &mut R) -> Result<usize> {
    // max redis length is 512MB, which is 9 bytes in ASCII + 2 for \r\n, +1 for the prefix
    let mut buffer = String::with_capacity(12);
    reader
        .read_line(&mut buffer)
        .context("failed to read prefixed length")?;

    let read_prefix = &buffer[0..prefix.len()];
    ensure!(
        prefix == read_prefix,
        "expected prefix {} but got {}",
        prefix,
        read_prefix
    );

    let len = buffer[prefix.len()..buffer.len() - 2]
        .parse::<usize>()
        .context("failed to parse prefixed length")?;

    Ok(len)
}

pub fn write_bulk_string<W: Write>(value: &str, writer: &mut W) -> Result<()> {
    write!(writer, "${}\r\n{}\r\n", value.len(), value).context("failed to write bulk string")?;
    writer.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufReader, Cursor};

    #[test]
    fn test_read_array_of_strings() {
        let input = Cursor::new("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        let result = read_array_of_bulk_strings(&mut BufReader::new(input)).unwrap();
        assert_eq!(result, vec!["foo".to_string(), "bar".to_string()]);
    }

    #[test]
    fn test_read_bulk_string() {
        let input = Cursor::new("$5\r\nhello\r\n");
        let result = read_bulk_string(&mut BufReader::new(input)).unwrap();
        assert_eq!(result, "hello");
    }

    #[test]
    fn test_read_prefixed_length() {
        let input = Cursor::new("$5\r\n");
        let result = read_prefixed_length("$", &mut BufReader::new(input)).unwrap();
        assert_eq!(result, 5);
    }
}
