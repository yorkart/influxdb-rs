use std::fmt::Debug;

use crate::engine::tsm1::block::decoder::{
    decode_bool_block, decode_float_block, decode_integer_block, decode_string_block,
    decode_unsigned_block,
};
use crate::engine::tsm1::block::{
    BLOCK_BOOLEAN, BLOCK_FLOAT64, BLOCK_INTEGER, BLOCK_STRING, BLOCK_UNSIGNED,
};

pub trait FieldType: Send + Sync + Sized + Debug + PartialOrd + PartialEq + Default {}

impl FieldType for f64 {}
impl FieldType for i64 {}
impl FieldType for bool {}
impl FieldType for Vec<u8> {}
impl FieldType for u64 {}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct Value<T>
where
    T: FieldType,
{
    pub unix_nano: i64,
    pub value: T,
}

impl<T> Value<T>
where
    T: FieldType,
{
    pub fn new(unix_nano: i64, value: T) -> Self {
        Self { unix_nano, value }
    }
}

pub trait TValue: Debug + Send + Clone + PartialOrd + PartialEq {
    fn block_type() -> u8;
    fn encode_size(&self) -> usize;
    fn decode(values: &mut Vec<Self>, block: &[u8]) -> anyhow::Result<()>;
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

    fn decode(values: &mut Vec<Self>, block: &[u8]) -> anyhow::Result<()> {
        decode_float_block(block, values)
    }
}

impl TValue for IntegerValue {
    fn block_type() -> u8 {
        BLOCK_INTEGER
    }

    fn encode_size(&self) -> usize {
        16
    }

    fn decode(values: &mut Vec<Self>, block: &[u8]) -> anyhow::Result<()> {
        decode_integer_block(block, values)
    }
}

impl TValue for UnsignedValue {
    fn block_type() -> u8 {
        BLOCK_UNSIGNED
    }

    fn encode_size(&self) -> usize {
        16
    }

    fn decode(values: &mut Vec<Self>, block: &[u8]) -> anyhow::Result<()> {
        decode_unsigned_block(block, values)
    }
}

impl TValue for BoolValue {
    fn block_type() -> u8 {
        BLOCK_BOOLEAN
    }

    fn encode_size(&self) -> usize {
        9
    }

    fn decode(values: &mut Vec<Self>, block: &[u8]) -> anyhow::Result<()> {
        decode_bool_block(block, values)
    }
}

impl TValue for StringValue {
    fn block_type() -> u8 {
        BLOCK_STRING
    }

    fn encode_size(&self) -> usize {
        8 + self.value.len()
    }

    fn decode(values: &mut Vec<Self>, block: &[u8]) -> anyhow::Result<()> {
        decode_string_block(block, values)
    }
}
