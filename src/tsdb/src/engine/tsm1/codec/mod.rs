use influxdb_utils::time::{time_format, unix_nano_to_time};

pub mod bit;
pub mod simple8b;
pub mod varint;
pub mod zigzag;

pub mod boolean;
pub mod float;
pub mod integer;
// pub mod number;
pub mod string;
pub mod timestamp;

/// BLOCK_FLOAT64 designates a block encodes float64 values.
pub const BLOCK_FLOAT64: i8 = 0;

/// BLOCK_INTEGER designates a block encodes int64 values.
pub const BLOCK_INTEGER: i8 = 1;

/// BLOCK_BOOLEAN designates a block encodes boolean values.
pub const BLOCK_BOOLEAN: i8 = 2;

/// BLOCK_STRING designates a block encodes string values.
pub const BLOCK_STRING: i8 = 3;

/// BLOCK_UNSIGNED designates a block encodes uint64 values.
pub const BLOCK_UNSIGNED: i8 = 4;

/// ENCODED_BLOCK_HEADER_SIZE is the size of the header for an encoded block.  There is one
/// byte encoding the type of the block.
const ENCODED_BLOCK_HEADER_SIZE: usize = 1;

/// `BlockData` describes the various types of block data that can be held
/// within a TSM file.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Float { unix_nano: i64, value: f64 },
    Integer { unix_nano: i64, value: i64 },
    Bool { unix_nano: i64, value: bool },
    Str { unix_nano: i64, value: Vec<u8> },
    Unsigned { unix_nano: i64, value: u64 },
}

impl Value {
    fn size(&self) -> usize {
        match self {
            Self::Float { .. } => 16,
            Self::Integer { .. } => 16,
            Self::Bool { .. } => 9,
            Self::Str {
                unix_nano: _,
                value,
            } => value.len() + 8,
            Self::Unsigned { .. } => 16,
        }
    }

    fn string(&self) -> String {
        match self {
            Self::Float { unix_nano, value } => {
                format!("{} {}", time_format(unix_nano_to_time(*unix_nano)), value)
            }
            Self::Integer { unix_nano, value } => {
                format!("{} {}", time_format(unix_nano_to_time(*unix_nano)), value)
            }
            Self::Bool { unix_nano, value } => {
                format!("{} {}", time_format(unix_nano_to_time(*unix_nano)), value)
            }
            Self::Str { unix_nano, value } => {
                format!(
                    "{} {}",
                    time_format(unix_nano_to_time(*unix_nano)),
                    String::from_utf8_lossy(value)
                )
            }
            Self::Unsigned { unix_nano, value } => {
                format!("{} {}", time_format(unix_nano_to_time(*unix_nano)), value)
            }
        }
    }
}

pub fn new_integer_value(t: i64, v: i64) -> Value {
    Value::Integer {
        unix_nano: t,
        value: v,
    }
}

pub fn new_unsigned_value(t: i64, v: u64) -> Value {
    Value::Unsigned {
        unix_nano: t,
        value: v,
    }
}

pub fn new_float_value(t: i64, v: f64) -> Value {
    Value::Float {
        unix_nano: t,
        value: v,
    }
}

pub fn new_boolean_value(t: i64, v: bool) -> Value {
    Value::Bool {
        unix_nano: t,
        value: v,
    }
}

pub fn new_string_value(t: i64, v: Vec<u8>) -> Value {
    Value::Str {
        unix_nano: t,
        value: v,
    }
}

pub async fn encode_float_block(values: Vec<Value>, _dst: &mut Vec<u8>) -> anyhow::Result<()> {
    if values.len() == 0 {
        return Ok(());
    }

    Ok(())
}
