use std::fmt::Debug;

use crate::engine::tsm1::block::ENCODED_BLOCK_HEADER_SIZE;
use crate::engine::tsm1::block::{
    BLOCK_BOOLEAN, BLOCK_FLOAT64, BLOCK_INTEGER, BLOCK_STRING, BLOCK_UNSIGNED,
};
use crate::engine::tsm1::codec::boolean::BooleanDecoder;
use crate::engine::tsm1::codec::float::FloatDecoder;
use crate::engine::tsm1::codec::integer::IntegerDecoder;
use crate::engine::tsm1::codec::string::StringDecoder;
use crate::engine::tsm1::codec::timestamp::TimeDecoder;
use crate::engine::tsm1::codec::unsigned::UnsignedDecoder;
use crate::engine::tsm1::codec::varint::VarInt;
use crate::engine::tsm1::codec::{timestamp, Decoder};
use crate::engine::tsm1::encoding::{
    BooleanValues, Capacity, FloatValues, IntegerValues, StringValues, UnsignedValues, Value,
    Values,
};

pub fn decode_block(block: &[u8], values: &mut Values) -> anyhow::Result<()> {
    if block.len() <= ENCODED_BLOCK_HEADER_SIZE {
        return Err(anyhow!(
            "decode of short block: got {}, exp {}",
            block.len(),
            ENCODED_BLOCK_HEADER_SIZE
        ));
    }

    let (typ, tb, vb) = unpack_block(block)?;
    let sz = timestamp::count_timestamps(tb)?;

    match typ {
        BLOCK_FLOAT64 => {
            if let Values::Float(values) = values {
                decode_float_block_values(tb, vb, sz, values)
            } else {
                Err(anyhow!(
                    "invalid block type: exp {}, got {}",
                    BLOCK_FLOAT64,
                    typ
                ))
            }
        }
        BLOCK_INTEGER => {
            if let Values::Integer(values) = values {
                decode_integer_block_values(tb, vb, sz, values)
            } else {
                Err(anyhow!(
                    "invalid block type: exp {}, got {}",
                    BLOCK_INTEGER,
                    typ
                ))
            }
        }
        BLOCK_BOOLEAN => {
            if let Values::Bool(values) = values {
                decode_bool_block_values(tb, vb, sz, values)
            } else {
                Err(anyhow!(
                    "invalid block type: exp {}, got {}",
                    BLOCK_BOOLEAN,
                    typ
                ))
            }
        }
        BLOCK_STRING => {
            if let Values::String(values) = values {
                decode_string_block_values(tb, vb, sz, values)
            } else {
                Err(anyhow!(
                    "invalid block type: exp {}, got {}",
                    BLOCK_STRING,
                    typ
                ))
            }
        }
        BLOCK_UNSIGNED => {
            if let Values::Unsigned(values) = values {
                decode_unsigned_block_values(tb, vb, sz, values)
            } else {
                Err(anyhow!(
                    "invalid block type: exp {}, got {}",
                    BLOCK_UNSIGNED,
                    typ
                ))
            }
        }
        _ => return Err(anyhow!("unknown block type: {}", typ)),
    }
}

pub fn decode_float_block(block: &[u8], values: &mut FloatValues) -> anyhow::Result<()> {
    let (tb, vb, sz) = pre_decode(block, BLOCK_FLOAT64)?;
    decode_float_block_values(tb, vb, sz, values)
}

pub fn decode_integer_block(block: &[u8], values: &mut IntegerValues) -> anyhow::Result<()> {
    let (tb, vb, sz) = pre_decode(block, BLOCK_INTEGER)?;
    decode_integer_block_values(tb, vb, sz, values)
}

pub fn decode_bool_block(block: &[u8], values: &mut BooleanValues) -> anyhow::Result<()> {
    let (tb, vb, sz) = pre_decode(block, BLOCK_BOOLEAN)?;
    decode_bool_block_values(tb, vb, sz, values)
}

pub fn decode_string_block(block: &[u8], values: &mut StringValues) -> anyhow::Result<()> {
    let (tb, vb, sz) = pre_decode(block, BLOCK_STRING)?;
    decode_string_block_values(tb, vb, sz, values)
}

pub fn decode_unsigned_block(block: &[u8], values: &mut UnsignedValues) -> anyhow::Result<()> {
    let (tb, vb, sz) = pre_decode(block, BLOCK_UNSIGNED)?;
    decode_unsigned_block_values(tb, vb, sz, values)
}

fn pre_decode(block: &[u8], expect_typ: u8) -> anyhow::Result<(&[u8], &[u8], usize)> {
    if block.len() <= ENCODED_BLOCK_HEADER_SIZE {
        return Err(anyhow!(
            "decode of short block: got {}, exp {}",
            block.len(),
            ENCODED_BLOCK_HEADER_SIZE
        ));
    }

    let (typ, tb, vb) = unpack_block(block)?;
    if typ != expect_typ {
        return Err(anyhow!(
            "invalid block type: exp {}, got {}",
            expect_typ,
            typ
        ));
    }
    let sz = timestamp::count_timestamps(tb)?;

    Ok((tb, vb, sz))
}

fn decode_float_block_values(
    tb: &[u8],
    vb: &[u8],
    sz: usize,
    values: &mut FloatValues,
) -> anyhow::Result<()> {
    let ts_dec = TimeDecoder::new(tb)?;
    let v_dec = FloatDecoder::new(vb)?;
    decode_block_using(sz, ts_dec, v_dec, values)?;
    Ok(())
}

fn decode_integer_block_values(
    tb: &[u8],
    vb: &[u8],
    sz: usize,
    values: &mut IntegerValues,
) -> anyhow::Result<()> {
    let ts_dec = TimeDecoder::new(tb)?;
    let v_dec = IntegerDecoder::new(vb)?;
    decode_block_using(sz, ts_dec, v_dec, values)?;
    Ok(())
}

fn decode_bool_block_values(
    tb: &[u8],
    vb: &[u8],
    sz: usize,
    values: &mut BooleanValues,
) -> anyhow::Result<()> {
    let ts_dec = TimeDecoder::new(tb)?;
    let v_dec = BooleanDecoder::new(vb)?;
    decode_block_using(sz, ts_dec, v_dec, values)?;
    Ok(())
}

fn decode_string_block_values(
    tb: &[u8],
    vb: &[u8],
    sz: usize,
    values: &mut StringValues,
) -> anyhow::Result<()> {
    let ts_dec = TimeDecoder::new(tb)?;
    let v_dec = StringDecoder::new(vb)?;
    decode_block_using(sz, ts_dec, v_dec, values)?;
    Ok(())
}

fn decode_unsigned_block_values(
    tb: &[u8],
    vb: &[u8],
    sz: usize,
    values: &mut UnsignedValues,
) -> anyhow::Result<()> {
    let ts_dec = TimeDecoder::new(tb)?;
    let v_dec = UnsignedDecoder::new(vb)?;
    decode_block_using(sz, ts_dec, v_dec, values)?;
    Ok(())
}
fn decode_block_using<T>(
    sz: usize,
    mut ts_dec: impl Decoder<i64>,
    mut v_dec: impl Decoder<T>,
    values: &mut Vec<Value<T>>,
) -> anyhow::Result<()>
where
    T: Debug + Clone + PartialOrd + PartialEq,
    Value<T>: Capacity,
{
    let remain = values.capacity() - values.len();
    if remain < sz {
        values.reserve_exact(sz - remain);
    }

    for _ in 0..sz {
        if !ts_dec.next() {
            return Err(anyhow!("can not read all timestamp block"));
        }
        if let Some(err) = ts_dec.err() {
            return Err(anyhow!("read timestamp block error: {}", err.to_string()));
        }
        if !v_dec.next() {
            return Err(anyhow!("can not read all values block"));
        }
        if let Some(err) = v_dec.err() {
            return Err(anyhow!("read values block error: {}", err.to_string()));
        }

        values.push(Value::new(ts_dec.read(), v_dec.read()));
    }

    Ok(())
}

pub fn unpack_block(buf: &[u8]) -> anyhow::Result<(u8, &[u8], &[u8])> {
    if buf.len() == 0 {
        return Err(anyhow!("unpackBlock: no data found"));
    }

    // Unpack the type
    let typ = buf[0];

    // Unpack the timestamp block length
    let (ts_len, i) = u64::decode_var(buf).ok_or(anyhow!(
        "unpackBlock: unable to read timestamp block length"
    ))?;

    // Unpack the timestamp bytes
    let ts_idx = i + ts_len as usize;
    if ts_idx > buf.len() {
        return Err(anyhow!("unpackBlock: not enough data for timestamp"));
    }
    let ts = &buf[i..ts_idx];

    // Unpack the value bytes
    let values = &buf[ts_idx..];

    Ok((typ, ts, values))
}

/// block_type returns the type of value encoded in a block or an error
/// if the block type is unknown.
pub fn block_type(block: &[u8]) -> anyhow::Result<u8> {
    let block_type = block[0];
    match block_type {
        BLOCK_FLOAT64 | BLOCK_INTEGER | BLOCK_BOOLEAN | BLOCK_STRING | BLOCK_UNSIGNED => {
            Ok(block_type)
        }
        _ => Err(anyhow!("unknown block type: {}", block_type)),
    }
}

/// block_count returns the number of timestamps encoded in block.
pub fn block_count(block: &[u8]) -> anyhow::Result<usize> {
    if block.len() <= ENCODED_BLOCK_HEADER_SIZE {
        return Err(anyhow!(
            "decode of short block: got {}, exp {}",
            block.len(),
            ENCODED_BLOCK_HEADER_SIZE
        ));
    }

    let (_typ, tb, _vb) = unpack_block(block)?;
    timestamp::count_timestamps(tb)
}
