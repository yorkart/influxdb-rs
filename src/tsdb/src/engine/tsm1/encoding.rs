use influxdb_utils::time::{time_format, unix_nano_to_time};

/// Value represents a TSM-encoded value.
pub trait TValue<T>
where
    Self: Send + Sync,
    T: Send + Sync,
{
    /// UnixNano returns the timestamp of the value in nanoseconds since unix epoch.
    fn unix_nano(&self) -> i64;

    /// Value returns the underlying value.
    fn value(&self) -> &T;

    /// Size returns the number of bytes necessary to represent the value and its timestamp.
    fn size(&self) -> usize;

    /// String returns the string representation of the value and its timestamp.
    fn string(&self) -> String;
}

/// `BlockData` describes the various types of block data that can be held
/// within a TSM file.
#[derive(Debug, Clone, PartialEq)]
pub enum Values {
    Float(Vec<(i64, f64)>),
    Integer(Vec<(i64, i64)>),
    Bool(Vec<(i64, bool)>),
    Str(Vec<(i64, Vec<u8>)>),
    Unsigned(Vec<(i64, u64)>),
}

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
//
// pub fn encode_block(dst: &mut Vec<u8>, values: Values) -> anyhow::Result<Vec<u8>> {
//
//     match values {
//         Values::Float(values) => encode_float_block(dst, values),
//         Values::Integer(values) => encode_integer_block(dst, values),
//         Values::Bool(values) => encode_boolean_block(dst, values),
//         Values::Str(values) => encode_string_block(dst, values),
//         Values::Unsigned(values) => encode_unsigned_block(dst, values),
//     }
// }
//
// pub fn encode_float_block(buf: &mut [u8], values: Vec<(i64, f64)>) -> anyhow::Result<Vec<u8>> {
//     if values.len() == 0 {
//         return Err(anyhow!("no data found"));
//     }
//
//     // A float block is encoded using different compression strategies
//     // for timestamps and values.
//
//     // Encode values using Gorilla float compression
//     let venc = FloatEncoder::new();
//
//     // Encode timestamps using an adaptive encoder that uses delta-encoding,
//     // frame-or-reference and run length encoding.
//     let tsenc = TimeEncoder::new(values.len());
//
//     Ok(vec![])
// }
//
// pub fn encode_integer_block(buf: &mut [u8], values: Vec<(i64, i64)>) -> anyhow::Result<Vec<u8>> {
//     Ok(vec![])
// }
//
// pub fn encode_unsigned_block(buf: &mut [u8], values: Vec<(i64, u64)>) -> anyhow::Result<Vec<u8>> {
//     Ok(vec![])
// }
//
// pub fn encode_boolean_block(buf: &mut [u8], values: Vec<(i64, bool)>) -> anyhow::Result<Vec<u8>> {
//     Ok(vec![])
// }
//
// pub fn encode_string_block(buf: &mut [u8], values: Vec<(i64, Vec<u8>)>) -> anyhow::Result<Vec<u8>> {
//     Ok(vec![])
// }
