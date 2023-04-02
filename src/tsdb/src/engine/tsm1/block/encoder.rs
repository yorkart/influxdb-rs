use crate::engine::tsm1::block::{
    BLOCK_BOOLEAN, BLOCK_FLOAT64, BLOCK_INTEGER, BLOCK_STRING, BLOCK_UNSIGNED,
};
use crate::engine::tsm1::codec::boolean::BooleanEncoder;
use crate::engine::tsm1::codec::float::FloatEncoder;
use crate::engine::tsm1::codec::integer::IntegerEncoder;
use crate::engine::tsm1::codec::string::StringEncoder;
use crate::engine::tsm1::codec::timestamp::TimeEncoder;
use crate::engine::tsm1::codec::unsigned::UnsignedEncoder;
use crate::engine::tsm1::codec::varint::VarInt;
use crate::engine::tsm1::codec::{varint, Encoder};
use crate::engine::tsm1::encoding::{Value, Values};
use std::fmt::Debug;

pub fn encode_block(dst: &mut Vec<u8>, values: Values) -> anyhow::Result<Vec<u8>> {
    match values {
        Values::Float(values) => encode_float_block(dst, values),
        Values::Integer(values) => encode_integer_block(dst, values),
        Values::Bool(values) => encode_bool_block(dst, values),
        Values::Str(values) => encode_str_block(dst, values),
        Values::Unsigned(values) => encode_unsigned_block(dst, values),
    }
}

fn encode_float_block(buf: &mut Vec<u8>, values: Vec<Value<f64>>) -> anyhow::Result<Vec<u8>> {
    let v_enc = FloatEncoder::new();
    let ts_enc = TimeEncoder::new(values.len());
    encode_block_using(BLOCK_FLOAT64, buf, values, ts_enc, v_enc)
}

fn encode_integer_block(buf: &mut Vec<u8>, values: Vec<Value<i64>>) -> anyhow::Result<Vec<u8>> {
    let v_enc = IntegerEncoder::new(values.len());
    let ts_enc = TimeEncoder::new(values.len());
    encode_block_using(BLOCK_INTEGER, buf, values, ts_enc, v_enc)
}

fn encode_bool_block(buf: &mut Vec<u8>, values: Vec<Value<bool>>) -> anyhow::Result<Vec<u8>> {
    let v_enc = BooleanEncoder::new(values.len());
    let ts_enc = TimeEncoder::new(values.len());
    encode_block_using(BLOCK_BOOLEAN, buf, values, ts_enc, v_enc)
}

fn encode_str_block(buf: &mut Vec<u8>, values: Vec<Value<Vec<u8>>>) -> anyhow::Result<Vec<u8>> {
    let v_enc = StringEncoder::new(values.len());
    let ts_enc = TimeEncoder::new(values.len());
    encode_block_using(BLOCK_STRING, buf, values, ts_enc, v_enc)
}

fn encode_unsigned_block(buf: &mut Vec<u8>, values: Vec<Value<u64>>) -> anyhow::Result<Vec<u8>> {
    let v_enc = UnsignedEncoder::new(values.len());
    let ts_enc = TimeEncoder::new(values.len());
    encode_block_using(BLOCK_UNSIGNED, buf, values, ts_enc, v_enc)
}

fn encode_block_using<T>(
    typ: u8,
    buf: &mut Vec<u8>,
    values: Vec<Value<T>>,
    mut ts_enc: impl Encoder<i64>,
    mut v_enc: impl Encoder<T>,
) -> anyhow::Result<Vec<u8>>
where
    T: Debug + Clone + PartialOrd + PartialEq,
{
    if values.len() == 0 {
        return Err(anyhow!("encode_float_block: no data found"));
    }

    for Value { unix_nano, value } in values {
        ts_enc.write(unix_nano);
        v_enc.write(value);
    }

    v_enc.flush();

    // Encoded timestamp values
    let tb = ts_enc.bytes()?;
    // Encoded float values
    let vb = v_enc.bytes()?;

    // Prepend the first timestamp of the block in the first 8 bytes and the block
    // in the next byte, followed by the block
    pack_block(buf, typ, tb, vb)
}

pub fn pack_block(
    buf: &mut Vec<u8>,
    typ: u8,
    ts: Vec<u8>,
    values: Vec<u8>,
) -> anyhow::Result<Vec<u8>> {
    let sz = 1 + varint::MAX_VARINT_LEN64 + ts.len() + values.len();
    buf.reserve_exact(sz);

    buf.push(typ);
    let _ = ts.len().encode_var_vec(buf);

    buf.extend_from_slice(ts.as_slice());
    buf.extend_from_slice(values.as_slice());

    Ok(vec![])
}
