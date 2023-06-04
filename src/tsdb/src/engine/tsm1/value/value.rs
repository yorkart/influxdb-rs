use influxdb_utils::time::unix_nano_to_time;
use std::fmt::{Debug, Formatter};

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

#[derive(Clone, PartialEq, PartialOrd)]
pub struct TimeValue<T>
where
    T: FieldType,
{
    pub unix_nano: i64,
    pub value: T,
}

impl<T> TimeValue<T>
where
    T: FieldType,
{
    pub fn new(unix_nano: i64, value: T) -> Self {
        Self { unix_nano, value }
    }
}

impl<T> Debug for TimeValue<T>
where
    T: FieldType,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TimeValue")
            .field("unix_nano", &unix_nano_to_time(self.unix_nano))
            .field("value", &self.value)
            .finish()
    }
}

pub trait Value: Debug + Send + Clone + PartialOrd + PartialEq {
    fn block_type() -> u8;
    fn encode_size(&self) -> usize;
    fn decode(values: &mut Vec<Self>, block: &[u8]) -> anyhow::Result<()>;
}

pub type FloatValue = TimeValue<f64>;
pub type IntegerValue = TimeValue<i64>;
pub type BoolValue = TimeValue<bool>;
pub type StringValue = TimeValue<Vec<u8>>;
pub type UnsignedValue = TimeValue<u64>;

impl Value for FloatValue {
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

impl Value for IntegerValue {
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

impl Value for UnsignedValue {
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

impl Value for BoolValue {
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

impl Value for StringValue {
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
