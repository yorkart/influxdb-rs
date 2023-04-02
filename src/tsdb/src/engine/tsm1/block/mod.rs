use crate::engine::tsm1::block::float::encode_float_block;
use crate::engine::tsm1::codec::varint;
use crate::engine::tsm1::codec::varint::VarInt;
use crate::engine::tsm1::encoding::Values;

pub mod float;

/// BLOCK_FLOAT64 designates a block encodes float64 values.
pub const BLOCK_FLOAT64: u8 = 0;

/// BLOCK_INTEGER designates a block encodes int64 values.
pub const BLOCK_INTEGER: u8 = 1;

/// BLOCK_BOOLEAN designates a block encodes boolean values.
pub const BLOCK_BOOLEAN: u8 = 2;

/// BLOCK_STRING designates a block encodes string values.
pub const BLOCK_STRING: u8 = 3;

/// BLOCK_UNSIGNED designates a block encodes uint64 values.
pub const BLOCK_UNSIGNED: u8 = 4;

/// ENCODED_BLOCK_HEADER_SIZE is the size of the header for an encoded block.  There is one
/// byte encoding the type of the block.
const ENCODED_BLOCK_HEADER_SIZE: usize = 1;

pub fn encode_block(dst: &mut Vec<u8>, values: Values) -> anyhow::Result<Vec<u8>> {
    match values {
        Values::Float(values) => encode_float_block(dst, values),
        // Values::Integer(values) => encode_integer_block(dst, values),
        // Values::Bool(values) => encode_boolean_block(dst, values),
        // Values::Str(values) => encode_string_block(dst, values),
        // Values::Unsigned(values) => encode_unsigned_block(dst, values),
        _ => Ok(vec![]),
    }
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
