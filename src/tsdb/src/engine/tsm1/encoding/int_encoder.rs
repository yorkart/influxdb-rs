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

use anyhow::anyhow;
use bytes::BufMut;

use crate::engine::tsm1::encoding::simple8b_encoder;
use crate::engine::tsm1::encoding::varint_encoder::VarInt;
use crate::engine::tsm1::encoding::zigzag_encoder::zig_zag_decode;
use crate::engine::tsm1::encoding::zigzag_encoder::zig_zag_encode;

/// INT_UNCOMPRESSED is an uncompressed format using 8 bytes per point
const INT_UNCOMPRESSED: u8 = 0;
/// INT_COMPRESSED_SIMPLE is a bit-packed format using simple8b encoding
const INT_COMPRESSED_SIMPLE: u8 = 1;
/// INT_COMPRESSED_RLE is a run-length encoding format
const INT_COMPRESSED_RLE: u8 = 2;

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
        b.put_u64(self.values[0]);

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
        let encoded = {
            let sz = simple8b_encoder::encode_all(self.values[1..].as_mut())?;
            &self.values[1..sz + 1]
        };

        let mut b = Vec::with_capacity(1 + (encoded.len() + 1) * 8);
        // b.resize(b.capacity(), 0);

        // 4 high bits of first byte store the encoding type for the block
        b.push((INT_COMPRESSED_SIMPLE as u8) << 4);

        // Write the first value since it's not part of the encoded values
        b.put_u64(self.values[0]);

        // Write the encoded values
        for v in encoded {
            b.put_u64(*v);
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
            b.put_u64(*v);
        }
        Ok(b)
    }
}

/// IntegerDecoder decodes a byte slice into int64s.
pub struct IntegerDecoder<'a> {
    /// 240 is the maximum number of values that can be encoded into a single uint64 using simple8b
    values: [u64; 240],

    bytes: &'a [u8],
    step: usize,

    i: usize,
    n: usize,
    prev: i64,
    first: bool,

    /// The first value for a run-length encoded byte slice
    rle_first: u64,

    /// The delta value for a run-length encoded byte slice
    rle_delta: u64,
    encoding: u8,
    err: Option<anyhow::Error>,
}

impl<'a> IntegerDecoder<'a> {
    pub fn new(b: &'a [u8]) -> Self {
        let (encoding, step) = if bytes.len() > 0 {
            (b[0] >> 4, 1)
        } else {
            (0, 0)
        };
        Self {
            values: [0; 240],
            bytes: b,
            step,
            i: 0,
            n: 0,
            prev: 0,
            first: true,
            rle_first: 0,
            rle_delta: 0,
            encoding,
            err: None,
        }
    }
    pub fn set_bytes(&mut self, _: &[u8]) {}

    /// Next returns true if there are any values remaining to be decoded.
    pub fn next(&mut self) -> bool {
        if self.i >= self.n && self.bytes.len() == 0 {
            return false;
        }

        self.i += 1;

        if self.i >= self.n {
            match self.encoding {
                INT_UNCOMPRESSED => self.decode_uncompressed(),
                INT_COMPRESSED_SIMPLE => self.decode_packed(),
                INT_COMPRESSED_RLE => self.decode_rle(),
                _ => self.err = Some(anyhow!("unknown encoding {}", self.encoding)),
            }
        }
        self.err.is_none() && self.i < self.n
    }

    /// Error returns the last error encountered by the decoder.
    pub fn error(&self) -> Option<&anyhow::Error> {
        self.err.map(|x| &x)
    }

    /// Read returns the next value from the decoder.
    pub fn read(&mut self) -> i64 {
        match self.encoding {
            INT_UNCOMPRESSED => {
                zig_zag_decode(self.rle_first) + (self.i as i64) * zig_zag_decode(self.rle_delta)
            }
            _ => {
                let mut v = zig_zag_decode(self.values[self.i]);
                v = v + self.prev;
                self.prev = v;
                v
            }
        }
    }

    fn decode_rle(&mut self) {
        if self.bytes.len() == 0 {
            return;
        }

        if self.bytes.len() < 8 {
            self.err = Some(anyhow!(
                "IntegerDecoder: not enough data to decode RLE starting value"
            ));
            return;
        }

        let mut i = 0;
        let mut n = 0;

        // Next 8 bytes is the starting value
        let first = u64::from_be_bytes(self.bytes[i..i + 8].try_into().unwrap());
        i += 8;

        // Next 1-10 bytes is the delta value
        let r = u64::decode_var(&self.bytes[i..]);
        if r.is_none() {
            self.err = Some(anyhow!("IntegerDecoder: invalid RLE delta value"));
            return;
        }
        let (value, n) = r.unwrap();
        i += n;

        // Last 1-10 bytes is how many times the value repeats
        let r = u64::decode_var(&self.bytes[i..]);
        if r.is_none() {
            self.err = Some(anyhow!("IntegerDecoder: invalid RLE repeat value"));
            return;
        }
        let (count, _n) = r.unwrap();

        // Store the first value and delta value so we do not need to allocate
        // a large values slice.  We can compute the value at position d.i on
        // demand.
        self.rle_first = first;
        self.rle_delta = value;
        self.n = (count as usize) + 1;
        self.i = 0;

        // We've process all the bytes
        self.bytes.clear();
    }

    fn decode_packed(&mut self) {
        if self.bytes.len() == 0 {
            return;
        }

        if self.bytes.len() < 8 {
            self.err = Some(anyhow!(
                "IntegerDecoder: not enough data to decode packed value"
            ));
            return;
        }

        let v = u64::from_be_bytes(self.bytes[0..8].try_into().unwrap());
        // The first value is always unencoded
        if self.first {
            self.first = false;
            self.n = 1;
            self.values[0] = v;
        } else {
            let r = simple8b_encoder::decode(self.values.as_mut(), v);
            if r.is_err() {
                // Should never happen, only error that could be returned is if the the value to be decoded was not
                // actually encoded by simple8b encoder.
                self.err = Some(anyhow!(
                    "failed to decode value {}: {:?}",
                    v,
                    r.err().unwrap()
                ));
            }
            let n = r.unwrap();

            self.n = n
        }
        self.i = 0;
        self.bytes = self.bytes[8..];
    }

    fn decode_uncompressed(&mut self) {
        if self.bytes.len() == 0 {
            return;
        }

        if self.bytes.len() < 8 {
            self.err = Some(anyhow!(
                "IntegerDecoder: not enough data to decode uncompressed value"
            ));
            return;
        }

        let v = u64::from_be_bytes(self.bytes[0..8].try_into().unwrap());
        self.values[0] = v;
        self.i = 0;
        self.n = 1;
        self.bytes = self.bytes[8..];
    }
}

/// IntegerDecoder decodes a byte slice into int64s.
pub trait Decoder {
    fn next(&mut self) -> bool;
    fn read(&self) -> i64;
}

pub enum IntegerDecoder<'a> {
    RleDecoder(RleDecoder),
    PackedDecoder(PackedDecoder<'a>),
    UncompressedDecoder(UncompressedDecoder<'a>),
    EmptyDecoder(EmptyDecoder),
}

impl<'a> IntegerDecoder<'a> {
    pub fn new(b: &[u8]) -> anyhow::Result<Self> {
        if b.len() > 0 {
            let encoding = b[0] >> 4;
            let b = &b[1..];
            match encoding {
                INT_UNCOMPRESSED => Ok(IntegerDecoder::UncompressedDecoder(
                    UncompressedDecoder::new(b)?,
                )),
                INT_COMPRESSED_SIMPLE => Ok(IntegerDecoder::PackedDecoder(PackedDecoder::new(b)?)),
                INT_COMPRESSED_RLE => Ok(IntegerDecoder::RleDecoder(RleDecoder::new(b)?)),
                _ => Err(anyhow!("unknown encoding {}", encoding)),
            }
        } else {
            Ok(IntegerDecoder::EmptyDecoder(EmptyDecoder {}))
        }
    }
}

impl<'a> Decoder for IntegerDecoder<'a> {
    fn next(&mut self) -> bool {
        match self {
            Self::RleDecoder(d) => d.next(),
            Self::PackedDecoder(d) => d.next(),
            Self::UncompressedDecoder(d) => d.next(),
            Self::EmptyDecoder(d) => d.next(),
        }
    }

    fn read(&self) -> i64 {
        match self {
            Self::RleDecoder(d) => d.read(),
            Self::PackedDecoder(d) => d.read(),
            Self::UncompressedDecoder(d) => d.read(),
            Self::EmptyDecoder(d) => d.read(),
        }
    }
}

struct EmptyDecoder {}

impl Decoder for EmptyDecoder {
    fn next(&mut self) -> bool {
        false
    }

    fn read(&self) -> i64 {
        0
    }
}

struct RleDecoder {
    first: i64,
    delta: i64,
    repeat: u64,

    step: i64,
}

impl<'a> RleDecoder {
    pub fn new(bytes: &[u8]) -> anyhow::Result<Self> {
        if bytes.len() == 0 {
            return Err(anyhow!(
                "IntegerDecoder: empty data to decode RLE starting value"
            ));
        }
        if bytes.len() < 8 {
            return Err(anyhow!(
                "IntegerDecoder: not enough data to decode RLE starting value"
            ));
        }

        let mut i = 0;

        // Next 8 bytes is the starting value
        let first = u64::from_be_bytes(bytes[..8].try_into().unwrap());
        i += 8;

        // Next 1-10 bytes is the delta value
        let (delta, n) = u64::decode_var(&bytes[i..])
            .ok_or(anyhow!("IntegerDecoder: invalid RLE delta value"))?;
        i += n;

        // Last 1-10 bytes is how many times the value repeats
        let (repeat, _n) = u64::decode_var(&bytes[i..])
            .ok_or(anyhow!("IntegerDecoder: invalid RLE repeat value"))?;

        Ok(Self {
            first: zig_zag_decode(first),
            delta: zig_zag_decode(delta),
            repeat,
            step: -1,
        })
    }
}

impl Decoder for RleDecoder {
    fn next(&mut self) -> bool {
        self.step += 1;

        if self.step >= (self.repeat + 1) as i64 {
            return false;
        }

        if self.step > 0 {
            self.first += self.delta;
        }

        return true;
    }

    fn read(&self) -> i64 {
        self.first
    }
}

struct PackedDecoder<'a> {
    first: i64,

    bytes: &'a [u8],
    b_step: usize,

    values: [u64; 240],
    v_step: usize,
    v_len: usize,

    err: Option<anyhow::Error>,
}

impl<'a> PackedDecoder<'a> {
    pub fn new(bytes: &'a [u8]) -> anyhow::Result<Self> {
        if bytes.len() == 0 {
            return Err(anyhow!(
                "IntegerDecoder: empty data to decode packed starting value"
            ));
        }
        if bytes.len() < 8 {
            return Err(anyhow!(
                "IntegerDecoder: not enough data to decode packed starting value"
            ));
        }

        // Next 8 bytes is the starting value
        let first = u64::from_be_bytes(bytes[..8].try_into().unwrap());

        Ok(Self {
            first: zig_zag_decode(first),
            bytes,
            b_step: 0,
            values: [0; 240],
            v_step: 0,
            v_len: 0,
            err: None,
        })
    }
}

impl<'a> Decoder for PackedDecoder<'a> {
    fn next(&mut self) -> bool {
        if self.err.is_some() {
            return false;
        }

        if self.b_step == 0 {
            self.b_step = 8;
            return true;
        }

        if self.v_step < self.v_len {
            self.v_step += 1;
            self.first += zig_zag_decode(self.values[self.v_step]);
            return true;
        }

        if self.b_step == self.bytes.len() {
            return false;
        } else if self.b_step + 8 > self.bytes.len() {
            self.err = Some(anyhow!(
                "IntegerDecoder: not enough data to decode packed value"
            ));
            return false;
        }

        let v = u64::from_be_bytes(self.bytes[self.b_step..self.b_step + 8].try_into().unwrap());
        let r = simple8b_encoder::decode(self.values.as_mut(), v);
        if r.is_err() {
            // Should never happen, only error that could be returned is if the the value to be decoded was not
            // actually encoded by simple8b encoder.
            self.err = Some(anyhow!(
                "failed to decode value {}: {:?}",
                v,
                r.err().unwrap()
            ));
            return false;
        }
        self.v_len = r.unwrap();
        self.v_step = 0;

        self.first += zig_zag_decode(self.values[self.v_step]);
        self.b_step += 8;

        return true;
    }

    fn read(&self) -> i64 {
        self.first
    }
}

struct UncompressedDecoder<'a> {
    first: i64,

    bytes: &'a [u8],
    b_step: usize,

    err: Option<anyhow::Error>,
}

impl<'a> UncompressedDecoder<'a> {
    pub fn new(bytes: &'a [u8]) -> anyhow::Result<Self> {
        if bytes.len() == 0 {
            return Err(anyhow!(
                "IntegerDecoder: empty data to decode packed starting value"
            ));
        }
        if bytes.len() < 8 {
            return Err(anyhow!(
                "IntegerDecoder: not enough data to decode packed starting value"
            ));
        }

        // Next 8 bytes is the starting value
        let first = u64::from_be_bytes(bytes[..8].try_into().unwrap());

        Ok(Self {
            first: zig_zag_decode(first),
            bytes,
            b_step: 0,
            err: None,
        })
    }
}

impl<'a> Decoder for UncompressedDecoder<'a> {
    fn next(&mut self) -> bool {
        if self.b_step == 0 {
            self.b_step += 8;
            return true;
        }

        if self.b_step == self.bytes.len() {
            return false;
        } else if self.b_step + 8 > self.bytes.len() {
            self.err = Some(anyhow!(
                "IntegerDecoder: not enough data to decode packed value"
            ));
            return false;
        }

        let v = u64::from_be_bytes(self.bytes[self.b_step..self.b_step + 8].try_into().unwrap());
        self.first += zig_zag_decode(v);
        self.b_step += 8;

        true
    }

    fn read(&self) -> i64 {
        self.first
    }
}
