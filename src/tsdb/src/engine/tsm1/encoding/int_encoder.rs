//! Integer encoding uses two different strategies depending on the range of values in
//! the uncompressed data.  Encoded values are first encoding used zig zag encoding.
//! This interleaves positive and negative integers across a range of positive integers.
//!
//! For example, [-2,-1,0,1] becomes [3,1,0,2]. See
//! https://developers.google.com/protocol-buffers/docs/encoding?hl=en#signed-integers
//! for more information.
//!
//! If all the zig zag encoded values are less than 1 << 60 - 1, they are compressed using
//! simple8b encoding.  If any value is larger than 1 << 60 - 1, the values are stored uncompressed.
//!
//! Each encoded byte slice contains a 1 byte header followed by multiple 8 byte packed integers
//! or 8 byte uncompressed integers.  The 4 high bits of the first byte indicate the encoding type
//! for the remaining bytes.
//!
//! There are currently two encoding types that can be used with room for 16 total.  These additional
//! encoding slots are reserved for future use.  One improvement to be made is to use a patched
//! encoding such as PFOR if only a small number of values exceed the max compressed value range.  This
//! should improve compression ratios with very large integers near the ends of the int64 range.

use bytes::BufMut;

use crate::engine::tsm1::encoding::simple8b_encoder;
use crate::engine::tsm1::encoding::varint_encoder::VarInt;
use crate::engine::tsm1::encoding::zigzag_encoder::zig_zag_encode;

/// INT_UNCOMPRESSED is an uncompressed format using 8 bytes per point
const INT_UNCOMPRESSED: usize = 0;
/// INT_COMPRESSED_SIMPLE is a bit-packed format using simple8b encoding
const INT_COMPRESSED_SIMPLE: usize = 1;
/// INT_COMPRESSED_RLE is a run-length encoding format
const INT_COMPRESSED_RLE: usize = 2;

/// IntegerEncoder encodes int64s into byte slices.
pub struct IntegerEncoder {
    prev: i64,
    rle: bool,
    values: Vec<u64>,
}

impl IntegerEncoder {
    /// NewIntegerEncoder returns a new integer encoder with an initial buffer of values sized at sz.
    pub fn new(sz: usize) -> Self {
        Self {
            prev: 0,
            rle: true,
            values: Vec::with_capacity(sz),
        }
    }

    /// Flush is no-op
    pub fn flush(&self) {}

    /// Reset sets the encoder back to its initial state.
    pub fn reset(&mut self) {
        self.prev = 0;
        self.rle = true;
        self.values.clear();
    }

    /// Write encodes v to the underlying buffers.
    pub fn write(&mut self, v: i64) {
        // Delta-encode each value as it's written.  This happens before
        // ZigZagEncoding because the deltas could be negative.
        let delta = v - self.prev;
        self.prev = v;
        let enc = zig_zag_encode(delta);
        if self.values.len() > 1 {
            self.rle = self.rle && self.values[self.values.len() - 1] == enc;
        }

        self.values.push(enc);
    }

    /// Bytes returns a copy of the underlying buffer.
    pub fn bytes(&mut self) -> anyhow::Result<Vec<u8>> {
        // Only run-length encode if it could reduce storage size.
        if self.rle && self.values.len() > 2 {
            return self.encode_rle();
        }

        for v in self.values.as_slice() {
            // Value is too large to encode using packed format
            if *v > simple8b_encoder::MAX_VALUE {
                return self.encode_uncompressed();
            }
        }

        return self.encode_packed();
    }

    fn encode_rle(&self) -> anyhow::Result<Vec<u8>> {
        // Large varints can take up to 10 bytes.  We're storing 3 + 1
        // type byte.
        let mut b = Vec::with_capacity(31);

        // 4 high bits used for the encoding type
        b.put_u8((INT_COMPRESSED_RLE as u8) << 4);

        // The first value
        b.put_u64_le(self.values[0]);
        // The first delta
        let mut tmp = [0u8; 9];
        let mut sz = self.values[1].encode_var(&mut tmp);
        b.extend_from_slice(&tmp[..sz]);
        // The number of times the delta is repeated
        sz = ((self.values.len() - 1) as u64).encode_var(&mut tmp);
        b.extend_from_slice(&tmp[..sz]);

        Ok(b)
    }

    fn encode_packed(&mut self) -> anyhow::Result<Vec<u8>> {
        if self.values.len() == 0 {
            return Ok(vec![]);
        }

        // Encode all but the first value.  Fist value is written unencoded
        // using 8 bytes.
        let encoded = simple8b_encoder::encode_all(self.values[1..].as_mut())?;

        let mut b = Vec::with_capacity(1 + (encoded.len() + 1) * 8);
        // b.resize(b.capacity(), 0);

        // 4 high bits of first byte store the encoding type for the block
        b.push((INT_COMPRESSED_SIMPLE as u8) << 4);

        // Write the first value since it's not part of the encoded values
        b.put_u64_le(self.values[0]);

        // Write the encoded values
        for v in encoded {
            b.put_u64_le(*v);
        }

        Ok(b)
    }

    fn encode_uncompressed(&self) -> anyhow::Result<Vec<u8>> {
        if self.values.len() == 0 {
            return Ok(vec![]);
        }

        let mut b = Vec::with_capacity(1 + self.values.len() * 8);
        // 4 high bits of first byte store the encoding type for the block
        b.put_u8((INT_UNCOMPRESSED as u8) << 4);

        for v in &self.values {
            b.put_u64_le(*v);
        }
        Ok(b)
    }
}
