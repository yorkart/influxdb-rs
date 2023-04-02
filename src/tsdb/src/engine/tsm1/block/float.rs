use crate::engine::tsm1::block::{pack_block, unpack_block, BLOCK_FLOAT64};
use crate::engine::tsm1::codec::float::{FloatDecoder, FloatEncoder};
use crate::engine::tsm1::codec::timestamp::{TimeDecoder, TimeEncoder};
use crate::engine::tsm1::codec::{timestamp, Decoder, Encoder};

pub fn encode_float_block(buf: &mut Vec<u8>, values: Vec<(i64, f64)>) -> anyhow::Result<Vec<u8>> {
    if values.len() == 0 {
        return Err(anyhow!("encode_float_block: no data found"));
    }

    // A float block is encoded using different compression strategies
    // for timestamps and values.

    // Encode values using Gorilla float compression
    let v_enc = FloatEncoder::new();

    // Encode timestamps using an adaptive encoder that uses delta-encoding,
    // frame-or-reference and run length encoding.
    let ts_enc = TimeEncoder::new(values.len());

    encode_float_block_using(buf, values, ts_enc, v_enc)
}

fn encode_float_block_using(
    buf: &mut Vec<u8>,
    values: Vec<(i64, f64)>,
    mut ts_enc: TimeEncoder,
    mut v_enc: FloatEncoder,
) -> anyhow::Result<Vec<u8>> {
    for (unix_nano, value) in values {
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
    pack_block(buf, BLOCK_FLOAT64, tb, vb)
}

/// decode_float_block decodes the float block from the byte slice
/// and appends the float values to `a`.
pub fn decode_float_block(block: &[u8], values: &mut Vec<(i64, f64)>) -> anyhow::Result<()> {
    let (typ, tb, vb) = unpack_block(block)?;
    if typ != BLOCK_FLOAT64 {
        return Err(anyhow!(
            "invalid block type: exp {}, got {}",
            BLOCK_FLOAT64,
            typ
        ));
    }

    let sz = timestamp::count_timestamps(tb)?;
    values.reserve_exact(sz);

    let mut ts_dec = TimeDecoder::new(tb)?;
    let mut v_dec = FloatDecoder::new(vb)?;

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

        values.push((ts_dec.read(), v_dec.read()));
    }

    Ok(())
}
