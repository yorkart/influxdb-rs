use std::fmt::Debug;

use crate::engine::tsm1::block::{
    BLOCK_BOOLEAN, BLOCK_FLOAT64, BLOCK_INTEGER, BLOCK_STRING, BLOCK_UNSIGNED,
};

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct Value<T>
where
    T: Debug + Send + Clone + PartialOrd + PartialEq,
{
    pub unix_nano: i64,
    pub value: T,
}

impl<T> Value<T>
where
    T: Debug + Send + Clone + PartialOrd + PartialEq,
{
    pub fn new(unix_nano: i64, value: T) -> Self {
        Self { unix_nano, value }
    }
}

pub trait TValue: Debug + Send + Clone + PartialOrd + PartialEq {
    fn block_type() -> u8;
    fn encode_size(&self) -> usize;
}

pub type FloatValue = Value<f64>;
pub type IntegerValue = Value<i64>;
pub type BoolValue = Value<bool>;
pub type StringValue = Value<Vec<u8>>;
pub type UnsignedValue = Value<u64>;

impl TValue for FloatValue {
    fn block_type() -> u8 {
        BLOCK_FLOAT64
    }

    fn encode_size(&self) -> usize {
        16
    }
}

impl TValue for IntegerValue {
    fn block_type() -> u8 {
        BLOCK_INTEGER
    }

    fn encode_size(&self) -> usize {
        16
    }
}

impl TValue for UnsignedValue {
    fn block_type() -> u8 {
        BLOCK_UNSIGNED
    }

    fn encode_size(&self) -> usize {
        16
    }
}

impl TValue for BoolValue {
    fn block_type() -> u8 {
        BLOCK_BOOLEAN
    }

    fn encode_size(&self) -> usize {
        9
    }
}

impl TValue for StringValue {
    fn block_type() -> u8 {
        BLOCK_STRING
    }

    fn encode_size(&self) -> usize {
        8 + self.value.len()
    }
}
