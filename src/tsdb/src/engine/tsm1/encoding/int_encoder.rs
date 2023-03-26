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
        let (delta, _) = v.overflowing_sub(self.prev);
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
    pub fn new(b: &'a [u8]) -> anyhow::Result<Self> {
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

pub struct EmptyDecoder {}

impl Decoder for EmptyDecoder {
    fn next(&mut self) -> bool {
        false
    }

    fn read(&self) -> i64 {
        0
    }
}

pub struct RleDecoder {
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

pub struct PackedDecoder<'a> {
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

        if self.v_len > 0 && self.v_step < self.v_len - 1 {
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
        if self.v_len == 0 {
            self.err = Some(anyhow!("simple8b length is 0"));
            return false;
        }

        self.v_step = 0;

        self.first += zig_zag_decode(self.values[self.v_step]);
        self.b_step += 8;

        return true;
    }

    fn read(&self) -> i64 {
        self.first
    }
}

pub struct UncompressedDecoder<'a> {
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
        (self.first, _) = self.first.overflowing_add(zig_zag_decode(v));
        self.b_step += 8;

        true
    }

    fn read(&self) -> i64 {
        self.first
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::tsm1::encoding::int_encoder::{
        Decoder, IntegerDecoder, IntegerEncoder, INT_COMPRESSED_SIMPLE, INT_UNCOMPRESSED,
    };

    #[test]
    fn test_integer_encoder_no_values() {
        let mut enc = IntegerEncoder::new(0);
        let b = enc.bytes().unwrap();

        assert_eq!(b.len(), 0, "unexpected length: exp 0, got {}", b.len());

        let mut dec = IntegerDecoder::new(b.as_slice()).unwrap();
        assert_eq!(
            dec.next(),
            false,
            "unexpected next value: got true, exp false"
        );
    }

    #[test]
    fn test_integer_encoder_one() {
        let mut enc = IntegerEncoder::new(1);
        let v1 = 1_i64;

        enc.write(v1);
        let b = enc.bytes().unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            got, INT_COMPRESSED_SIMPLE,
            "encoding type mismatch: exp simple, got {}",
            got
        );

        let mut dec = IntegerDecoder::new(b.as_slice()).unwrap();
        assert_eq!(
            dec.next(),
            true,
            "unexpected next value: got true, exp false"
        );
        assert_eq!(
            dec.read(),
            v1,
            "read value mismatch: got {}, exp {}",
            dec.read(),
            v1
        );
    }

    #[test]
    fn test_integer_encoder_two() {
        let mut enc = IntegerEncoder::new(2);
        let v1 = 1;
        let v2 = 2;

        enc.write(v1);
        enc.write(v2);

        let b = enc.bytes().unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            got, INT_COMPRESSED_SIMPLE,
            "encoding type mismatch: exp simple, got {}",
            got
        );

        let mut dec = IntegerDecoder::new(b.as_slice()).unwrap();
        assert_eq!(
            dec.next(),
            true,
            "unexpected next value: got true, exp false"
        );
        assert_eq!(
            dec.read(),
            v1,
            "read value mismatch: got {}, exp {}",
            dec.read(),
            v1
        );

        assert_eq!(
            dec.next(),
            true,
            "unexpected next value: got true, exp false"
        );
        assert_eq!(
            dec.read(),
            v2,
            "read value mismatch: got {}, exp {}",
            dec.read(),
            v2
        );
    }

    #[test]
    fn test_integer_encoder_negative() {
        let mut enc = IntegerEncoder::new(3);
        let v1 = -2;
        let v2 = 0;
        let v3 = 1;

        enc.write(v1);
        enc.write(v2);
        enc.write(v3);

        let b = enc.bytes().unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            got, INT_COMPRESSED_SIMPLE,
            "encoding type mismatch: exp simple, got {}",
            got
        );

        let mut dec = IntegerDecoder::new(b.as_slice()).unwrap();
        assert_eq!(
            dec.next(),
            true,
            "unexpected next value: got true, exp false"
        );
        assert_eq!(
            dec.read(),
            v1,
            "read value mismatch: got {}, exp {}",
            dec.read(),
            v1
        );

        assert_eq!(
            dec.next(),
            true,
            "unexpected next value: got true, exp false"
        );
        assert_eq!(
            dec.read(),
            v2,
            "read value mismatch: got {}, exp {}",
            dec.read(),
            v2
        );

        assert_eq!(
            dec.next(),
            true,
            "unexpected next value: got true, exp false"
        );
        assert_eq!(
            dec.read(),
            v3,
            "read value mismatch: got {}, exp {}",
            dec.read(),
            v3
        );
    }

    #[test]
    fn test_integer_encoder_large_range() {
        let mut enc = IntegerEncoder::new(2);
        let v1 = i64::MIN;
        let v2 = i64::MAX;

        enc.write(v1);
        enc.write(v2);

        let b = enc.bytes().unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            got, INT_UNCOMPRESSED,
            "encoding type mismatch: exp uncompressed, got {}",
            got
        );

        let mut dec = IntegerDecoder::new(b.as_slice()).unwrap();
        assert_eq!(
            dec.next(),
            true,
            "unexpected next value: got true, exp false"
        );
        assert_eq!(
            dec.read(),
            v1,
            "read value mismatch: got {}, exp {}",
            dec.read(),
            v1
        );

        assert_eq!(
            dec.next(),
            true,
            "unexpected next value: got true, exp false"
        );
        assert_eq!(
            dec.read(),
            v2,
            "read value mismatch: got {}, exp {}",
            dec.read(),
            v2
        );
    }

    #[test]
    fn test_integer_encoder_uncompressed() {
        let mut enc = IntegerEncoder::new(3);
        let v1 = 0;
        let v2 = 1;
        let v3 = 1 << 60;

        enc.write(v1);
        enc.write(v2);
        enc.write(v3);

        let b = enc.bytes().unwrap();

        let exp = 25;
        assert_eq!(
            b.len(),
            exp,
            "length mismatch: got {}, exp {}",
            b.len(),
            exp
        );

        let got = b[0] >> 4;
        assert_eq!(
            got, INT_UNCOMPRESSED,
            "encoding type mismatch: exp uncompressed, got {}",
            got
        );

        let mut dec = IntegerDecoder::new(b.as_slice()).unwrap();
        assert_eq!(
            dec.next(),
            true,
            "unexpected next value: got true, exp false"
        );
        assert_eq!(
            dec.read(),
            v1,
            "read value mismatch: got {}, exp {}",
            dec.read(),
            v1
        );

        assert_eq!(
            dec.next(),
            true,
            "unexpected next value: got true, exp false"
        );
        assert_eq!(
            dec.read(),
            v2,
            "read value mismatch: got {}, exp {}",
            dec.read(),
            v2
        );

        assert_eq!(
            dec.next(),
            true,
            "unexpected next value: got true, exp false"
        );
        assert_eq!(
            dec.read(),
            v3,
            "read value mismatch: got {}, exp {}",
            dec.read(),
            v3
        );
    }

    #[test]
    fn test_integer_encoder_negative_uncompressed() {
        let values: [i64; 24] = [
            -2352281900722994752,
            1438442655375607923,
            -4110452567888190110,
            -1221292455668011702,
            -1941700286034261841,
            -2836753127140407751,
            1432686216250034552,
            3663244026151507025,
            -3068113732684750258,
            -1949953187327444488,
            3713374280993588804,
            3226153669854871355,
            -2093273755080502606,
            1006087192578600616,
            -2272122301622271655,
            2533238229511593671,
            -4450454445568858273,
            2647789901083530435,
            2761419461769776844,
            -1324397441074946198,
            -680758138988210958,
            94468846694902125,
            -2394093124890745254,
            -2682139311758778198,
        ];

        let mut enc = IntegerEncoder::new(256);
        for v in &values {
            enc.write(*v);
        }

        let b = enc.bytes().unwrap();
        let got = b[0] >> 4;
        assert_eq!(
            got, INT_UNCOMPRESSED,
            "encoding type mismatch: exp uncompressed, got {}",
            got
        );

        let mut dec = IntegerDecoder::new(b.as_slice()).unwrap();

        let mut i = 0;
        while dec.next() {
            assert_eq!(
                i > values.len(),
                false,
                "read too many values: got {}, exp {}",
                i,
                values.len()
            );
            assert_eq!(
                values[i],
                dec.read(),
                "read value {} mismatch: got {}, exp {}",
                i,
                dec.read(),
                values[i]
            );
            i += 1
        }

        assert_eq!(
            i,
            values.len(),
            "failed to read enough values: got {}, exp {}",
            i,
            values.len()
        );
    }

    #[test]
    fn test_integer_encoder_all_negative() {
        let mut enc = IntegerEncoder::new(3);
        let values: [i64; 3] = [-10, -5, -1];

        for v in &values {
            enc.write(*v);
        }

        let b = enc.bytes().unwrap();
        let got = b[0] >> 4;
        assert_eq!(
            got, INT_COMPRESSED_SIMPLE,
            "encoding type mismatch: exp compressed_simple, got {}",
            got
        );

        let mut dec = IntegerDecoder::new(b.as_slice()).unwrap();

        let mut i = 0;
        while dec.next() {
            assert_eq!(
                i > values.len(),
                false,
                "read too many values: got {}, exp {}",
                i,
                values.len()
            );
            assert_eq!(
                values[i],
                dec.read(),
                "read value {} mismatch: got {}, exp {}",
                i,
                dec.read(),
                values[i]
            );
            i += 1
        }

        assert_eq!(
            i,
            values.len(),
            "failed to read enough values: got {}, exp {}",
            i,
            values.len()
        );
    }

    #[test]
    fn test_integer_encoder_counter_packed() {
        let mut enc = IntegerEncoder::new(16);
        let values: [i64; 6] = [
            1000000000000000,
            1000000000000000 + 1,
            1000000000000000 + 2,
            1000000000000000 + 3,
            1000000000000000 + 4,
            1000000000000000 + 6,
        ];

        for v in &values {
            enc.write(*v);
        }

        let b = enc.bytes().unwrap();
        let got = b[0] >> 4;
        assert_eq!(
            got, INT_COMPRESSED_SIMPLE,
            "encoding type mismatch: exp compressed_simple, got {}",
            got
        );
        let exp = 17;
        assert_eq!(
            b.len(),
            exp,
            "length mismatch: got {}, exp {}",
            b.len(),
            exp
        );

        let mut dec = IntegerDecoder::new(b.as_slice()).unwrap();

        let mut i = 0;
        while dec.next() {
            assert_eq!(
                i > values.len(),
                false,
                "read too many values: got {}, exp {}",
                i,
                values.len()
            );
            assert_eq!(
                values[i],
                dec.read(),
                "read value {} mismatch: got {}, exp {}",
                i,
                dec.read(),
                values[i]
            );
            i += 1
        }

        assert_eq!(
            i,
            values.len(),
            "failed to read enough values: got {}, exp {}",
            i,
            values.len()
        );
    }
}
