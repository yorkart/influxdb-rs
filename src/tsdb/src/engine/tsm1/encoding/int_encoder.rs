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
    fn err(&self) -> Option<&anyhow::Error>;
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

    fn err(&self) -> Option<&anyhow::Error> {
        match self {
            Self::RleDecoder(d) => d.err(),
            Self::PackedDecoder(d) => d.err(),
            Self::UncompressedDecoder(d) => d.err(),
            Self::EmptyDecoder(d) => d.err(),
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

    fn err(&self) -> Option<&anyhow::Error> {
        None
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

    fn err(&self) -> Option<&anyhow::Error> {
        None
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

    fn err(&self) -> Option<&anyhow::Error> {
        self.err.as_ref()
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

    fn err(&self) -> Option<&anyhow::Error> {
        self.err.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::tsm1::encoding::int_encoder::{
        Decoder, IntegerDecoder, IntegerEncoder, INT_COMPRESSED_RLE, INT_COMPRESSED_SIMPLE,
        INT_UNCOMPRESSED,
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
        let exp = 17;
        assert_eq!(
            b.len(),
            exp,
            "length mismatch: got {}, exp {}",
            b.len(),
            exp
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

    #[test]
    fn test_integer_encoder_counter_rle() {
        let mut enc = IntegerEncoder::new(16);
        let values: [i64; 6] = [
            1000000000000000,
            1000000000000000 + 1,
            1000000000000000 + 2,
            1000000000000000 + 3,
            1000000000000000 + 4,
            1000000000000000 + 5,
        ];

        for v in &values {
            enc.write(*v);
        }

        let b = enc.bytes().unwrap();
        let got = b[0] >> 4;
        assert_eq!(
            got, INT_COMPRESSED_RLE,
            "encoding type mismatch: exp compressed_rle, got {}",
            got
        );
        let exp = 11;
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

    #[test]
    fn test_integer_encoder_descending() {
        let mut enc = IntegerEncoder::new(16);
        let values: [i64; 3] = [7094, 4472, 1850];

        for v in &values {
            enc.write(*v);
        }

        let b = enc.bytes().unwrap();
        let got = b[0] >> 4;
        assert_eq!(
            got, INT_COMPRESSED_RLE,
            "encoding type mismatch: exp compressed_rle, got {}",
            got
        );
        let exp = 12;
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

    #[test]
    fn test_integer_encoder_flat() {
        let mut enc = IntegerEncoder::new(16);
        let values: [i64; 4] = [1, 1, 1, 1];

        for v in &values {
            enc.write(*v);
        }

        let b = enc.bytes().unwrap();
        let got = b[0] >> 4;
        assert_eq!(
            got, INT_COMPRESSED_RLE,
            "encoding type mismatch: exp compressed_rle, got {}",
            got
        );
        let exp = 11;
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

    #[test]
    fn test_integer_encoder_quick() {
        let test_data: Vec<Vec<i64>> = vec![
            vec![
                7831057746046091144,
                4572639390035410771,
                -3553819261794070419,
                -6078065176584368207,
                4583622430431469365,
                -2976694467874550611,
                -3908304219617694823,
                2091619580788110978,
                557703375288782401,
                -8882428348318633964,
                740148426045830818,
                8575602389518967823,
                2594131684909661512,
                -3123289630548424922,
                -6826053105219055435,
                -8464895235665563446,
                3307169345587960991,
            ],
            vec![
                647131473979010859,
                6958688210517996612,
                8184904981303934088,
                8127297573896253958,
                -2692995952186541667,
                -2795282360686069533,
                5642863578048541093,
                5159684867664582280,
                1777426381780179941,
                3123064933258332226,
                1495021223582209329,
                -9079474550218085964,
                -6970533624456990645,
                961618417195052709,
                6712496655732181890,
                -1688770528811417542,
                4692593537605847860,
                1098694097852339068,
                -810194492633456377,
                -5288941769630577000,
                -5673811848547578383,
                2360500394982024007,
                8420168367595934940,
                6405104578053470415,
                -4274653155751099651,
                5129251542727601879,
                3276390324219534010,
                -3566584585107627942,
                2862321174671634841,
                7742098934943429689,
                -4939473438388102372,
                -6205610110433391494,
                -52727688587474377,
                608031466967520603,
                7454337511601383692,
                -1242034148465563135,
                -748298170468887223,
                6075089754758063605,
                7996680782896137319,
                -4413483086253064746,
                2289420681328993323,
                5076017973663323041,
                7537242327688800856,
                -3234203769403360943,
                5532683454592517319,
                7640315058613888346,
            ],
            vec![
                -1510974373578978470,
                6271151315559198123,
                2147850419311936825,
                -5445215964928370203,
                -6124438186316884753,
                695190154158805371,
                116539628242319840,
                -4279525809268512941,
                7836395374533287170,
                -7204263580420820789,
            ],
            vec![
                -7457017672625509727,
                -4864406676358167737,
                -488718350274777066,
                -1716459802684484567,
                -5967348073610700365,
                8863519002999565068,
                -4161914885141596403,
                7355701620696108450,
                -4469576835748728705,
                -1051463540395032242,
                5657359167377574870,
                -5789360049816946905,
                6370018315333594970,
                1082402444381731011,
                2578273561012904467,
                -2510946790816581081,
                6288274459814697245,
                -6635399554025192953,
                4823493393707174231,
                -2993282546140385584,
                9075033547439982008,
                8126657470983643462,
                -5242644368770948147,
                -2859781957294216855,
                669666617355138024,
                -6310317965897238141,
                -1576418419830102219,
                4469623448302703120,
                -8587248692413142812,
                -8663409778343496134,
                -542855776019473920,
                -6986591128474611345,
                2012856252057858195,
                -6653779118460413717,
                -5356658189567890404,
                -1608511245456960238,
                -2374327805409922507,
                -5386931646878048005,
            ],
            vec![
                8922077410446755301,
                -7803376527578587820,
                320773253820148846,
                -2066910776216853461,
                -1275879861385957140,
                8067890894231970970,
                5050744676402457310,
                -4503020484530971040,
                -5688853895198078182,
                3024011699411042711,
                3977318239831110153,
                -4615489056398884103,
                3131283348696395064,
                4043483230659558926,
                -1762015217853083762,
            ],
            vec![
                -6055538649344354509,
                -8104364026114449912,
                -3216212606911023341,
            ],
            vec![
                -3350383663201962388,
                6369126898157489804,
                -46603942118431739,
                -6481067258440364531,
                8222417597886530015,
                -1960375488271783299,
                6610685783387927753,
                -8466647282935137807,
                -3780739970173120042,
                7492605546657796292,
                7348741645059924275,
                -9168065241671418475,
                9000199284009311318,
                4500037110545982677,
                7235073883793490248,
                8018808806944384654,
                5096347839846796984,
                7298914335955524218,
                -2470601475953657550,
                2645081377137999834,
                7504369376784941062,
                8014662550511865789,
                -2389540438827926604,
                3445113353990830325,
                -6268364042236161914,
                6276643219996100559,
                -7430539964527577886,
                -6205674361344775764,
                -5321988753535126464,
                7150649156562604174,
            ],
            vec![
                1401766262116930593,
                4553807215138241027,
                -1808487218222525729,
                8870636704820076270,
                6736708411580350415,
                2171119055456503199,
            ],
            vec![
                5961108424875505201,
                7831660482735426590,
                -9054203834234732925,
                5034941882266222224,
                5010212079837596352,
                -4923105075065894491,
                8619935377280000592,
                2848571632858363731,
                -2402534543098767338,
                -4781322463165052883,
                -1165746445545094216,
                7129856143904303015,
                6160328788688917019,
                -1662594792097672538,
                7966807337708574176,
                -3208479530331343498,
                -5186099141747836205,
                165572914352563765,
                8741429459396781918,
                297238526350103410,
                -6888355895547954203,
                -7977477270762321671,
                -6073898411470573763,
                3303134026694954780,
                5981916013710119141,
                -5071347057721928047,
                5316341846073483109,
                4400032481590664532,
                -3506931754815224842,
            ],
            vec![
                8431475463775586187,
                -6646360005165095562,
                -4949792632689147110,
                -6392144452522279770,
                1959801049536727935,
                1729739015561581070,
                -7817135679998017889,
                -4966869050031383810,
                -5184355329521557446,
                -4370884815859151102,
                73911913034913907,
                5416331204252392345,
                8004721055501976587,
                -5074608047220589759,
                -3583555244004722926,
                1644551412359419895,
                -3480689047989126338,
                4588146754234170468,
                544703110415170014,
                -3639224134860713922,
                -3373687540770775436,
                8454268980150202125,
                -3255499857322458405,
                -3313271674151673069,
                1795166260208648324,
                -2260188750636551616,
                -6710639151533266478,
                2558287799624039101,
                2288614574694986235,
                2161949104781091974,
                759504470333092361,
                -2885343551161512482,
                1696736330957619893,
                8020099679699297700,
                -6346097672614535556,
                2021897090421750784,
                5869247618790910301,
                8655356413089521783,
                -4561715434716910861,
                5211328595904512460,
                2339459781755525406,
            ],
            vec![
                8793931697411492134,
                -8213208491216823040,
                90288292897048303,
                -8216081127654390049,
                6253156292653946788,
                5832628929862777830,
                6228020293722527755,
                3795143413796621392,
                6900416216674826736,
                3994985024829400054,
                5135512943036903592,
                8533379740621489795,
                5968717561568263809,
                -8222321966102050492,
                1304878925711125109,
                2494253795822660719,
                -8972246886298936843,
                -6217613383685382983,
                2464486154945947503,
                -97464710260299875,
            ],
            vec![
                -3762559521265748854,
                -1928682810551336754,
                4560867711769315711,
                3387015743093774116,
                -2896652365806965608,
                4031988883757283084,
                6803490657117391711,
                -4076803915025058747,
                -1982464577437263111,
                3010194232872071804,
                -4527283480191085660,
                6616211544629299937,
                -5830226926771230564,
                6427308731685840142,
                -1978389832501559241,
                -4148720616837383636,
                8952816319404001804,
                737777971101247087,
                -220839194424404137,
            ],
            vec![
                928783554821025482,
                -4044860439780121381,
                -3504218314339775570,
                -4246937030863302620,
                3727949232795746998,
                -4081332214939524338,
                5894581762643977203,
                -4946697474355615947,
                -570190766667817322,
                3427319839301666313,
                -4137502172747504489,
                9056177285977418123,
                3519001019943586280,
            ],
            vec![
                -7657839245958191081,
                5364202026413108405,
                6894162930191691985,
                -9019103438029457587,
                -1847743987747917241,
                -2159475495629021941,
                4809046734331215647,
                367836620650190849,
                -2856498369529347952,
                -4904211937616624069,
                -5928104636697937005,
                2761911028778080022,
                4683397737891217926,
                -7803395072253117359,
                4470321451938274025,
                3351461598780508382,
                -7250189244643891493,
                6266543823793674754,
                8377141633766269649,
                -4092898705509618585,
                7303574346987411889,
                3302963004591335172,
                6522276656739944501,
                -5883857320831471282,
                3652002927355014017,
                -4641085078572023673,
                -3008703862054832261,
                1263663948449144814,
                2132485436598808027,
                -758130372927710285,
                4491668764091764333,
                7154887109254371604,
                6782204097189007674,
                1956743606325659733,
                -4817629546784452816,
                -6535605889534248601,
                -3177274362338160840,
                7229916887429505536,
                -9043342830052275762,
                2437589371825150146,
            ],
            vec![
                -922905668417045810,
                -4777381791767912625,
                -7554114196082996806,
                2433064188443373632,
                -5746906465966894273,
                3101266078978871907,
                -5734455201118069750,
                413573334482524129,
                7979588958638859335,
                904430277728437647,
                -3226558981460926796,
                5176031562311855931,
                -388397070966350431,
                3778498405728727728,
                -3512584796820148925,
                -2028725026751699510,
                -4416546225768908219,
                -1519404870919641267,
                2684918476455208359,
                -7692809361731225439,
                6231731559093173984,
                690223082619180757,
                2840599873695532432,
                -7663519263974135604,
                5897013414362136277,
                1695208897092659815,
                -7867475760897437434,
                2089091017810006439,
                5412275135734048633,
                8863612571248573298,
                -216145728778582540,
                -3501628333413349142,
                -698382946339691526,
            ],
            vec![
                -1494545708189208366,
                -7688190831471183359,
                7184248444928808737,
                7035460905263957709,
                -8542744716314998713,
                4295892546648897586,
                -2862100381726753898,
                2299983099222730577,
                8787994557226159821,
                -8160420845245263836,
                6499806542053693963,
                -3502023781662585828,
                2425597424752881166,
                -1001466965687137670,
                -3266542012570195897,
                -3444669936536698285,
                -5666337641269164703,
                -5442430752876932112,
                -3677218251281993967,
                7215365257530722443,
                557435096727531289,
                8985846823674269121,
                6087389039974712166,
                -2641467199703344514,
                4782652457520337531,
                5371126608911417073,
                -3637298060122189284,
                -9169884838578333839,
                -3837076910872114636,
                4149370042348688446,
                -1431681877599999054,
                -6197077128046409700,
                -1631842076793539683,
                6534978989402504972,
                4233174556997788647,
                6522030586624283335,
                1436539011129039436,
                8638462230353147890,
            ],
            vec![
                -4019357165409484984,
                -7497599270667049871,
                -9137996163154851322,
                -2110761834891564087,
                -7140326196479533737,
                8745089828833147788,
                -103390037464774527,
                -1054088038424866289,
                2786529653582342461,
                -1421068944676455607,
                1029156781324968840,
                5491163696069653306,
                -4861360006824098326,
                6999008806209567178,
                918899195747982225,
                8483201453016076902,
                6544591778698967506,
                4709776217930930107,
                6547317214750503374,
                -1066249076885425960,
                2392882244280249809,
                -6242617100056133489,
                3322801444506572464,
                -8974317113560562124,
                -3045470532097173029,
                1281041324614139196,
                -5147980904782768482,
                8525364918488580596,
                6032098059516011475,
                5028463748125990193,
                6101711855484767025,
                3764860618394061211,
                9068133600718711868,
                1641351606690866356,
            ],
            vec![
                -8415061278851228154,
                8328621451271863597,
                -7716982150831731216,
                -4082419092363271303,
                2216256472218096857,
                -1261815992873507373,
                -8495264073376060092,
                7340555958133353415,
                2535025818842072757,
                -2355462821226144479,
                -1792110347342414479,
                5910951387594610231,
                2362132950213117950,
                1662266313009215120,
                8547014108155169871,
                7872871099302670864,
                -6895393362768429753,
                -3233238775568209154,
                -5139596458731638645,
                -225136399973296672,
                -8979652544239224821,
                -333113230564604931,
                9017924331639620536,
                -3317746972959339170,
                5322894286970398466,
                8197425432945037423,
                4962335258817503402,
                -8136695126938330919,
                -4225951863777991641,
                -8022577391753220947,
                -2638619352415450903,
                -7997717040331930001,
            ],
            vec![],
            vec![
                -7636091806520308176,
                -4802062447482941212,
                7211192237468130071,
                -1997571731001265358,
                6640039354705312994,
                -4118534094417656559,
                1745029410911009539,
                3929588647257136786,
                7929073278932656498,
                -6763580644594329250,
                -2050716836713129773,
                -3903031763787308349,
                -2345778184605634528,
                -3499451489274276100,
                3922205041358862647,
                576619454572587262,
                7712187621289225473,
                2447097100689894994,
                -723881430074590517,
                -8264492213837107159,
                -2118999529710022849,
                7585135361805068774,
                1418946226379144824,
                -3329271061961969516,
                -5380475546090210238,
                2096745408222703582,
                -1082013202829949297,
                660526893148999356,
                4041113231650781885,
                -5100799046050853474,
                1345324523245416632,
                5351663793481850423,
                5312559745453327480,
                7954410770920345801,
            ],
            vec![
                -1463766217143103349,
                -8875445516715215802,
                6240260062262118739,
                100036595480085075,
                -462013128297396076,
            ],
            vec![
                -2100381486234363021,
                7934658288698836935,
                6412048151440805238,
                -2711894167270604584,
                5370387670253267377,
                -5864980256471550855,
                2049541476938741992,
                1941133132288644803,
                734020997531570670,
                -9187333717340144723,
                -2981045578036189294,
                -9002817850470879981,
                5388399764625023890,
                -2092448365881099336,
                -7544028311928251154,
                -364908723829980764,
                3166836024065132621,
                -1348453524416757512,
                -415008102703777898,
                1449140940401979115,
                -2829056340418309969,
                6208359943400165377,
                7929309513598894399,
                -519012210848201352,
                4723604390239382421,
                6360505876463484656,
                8557776548897661043,
                202064604316429095,
                2115407007139178118,
                -7313959757213865663,
                123081927691739719,
                -229828450021353950,
                37427245739885114,
                1306704569738937404,
                -6547340029510598136,
                6374326523607218777,
                6026598533469343120,
                -7031277558617016395,
                -1868640074052242284,
                6141386240883193640,
                -6499952484941031450,
                -5523544332350531873,
                -5423585820224473146,
                3943411931786273468,
                -7963791586408076524,
                3576492599311624281,
            ],
            vec![2174993534773012081, 7455592684381770906],
            vec![
                4526503625806461866,
                -47302035863831334,
                -5297557505023291180,
                -147130787981623637,
                -6451520932658549256,
                6149167956438017801,
                5791423581809169354,
                -1639526565618205899,
                4041627030388829333,
                7177132131221757173,
                -1991045327065176727,
                -5908957201139004265,
            ],
            vec![
                648319911662673726,
                -4490903005192103857,
                -1726637923051671086,
                2483123731888418114,
                3713741178602960825,
                -6038322887920016459,
                4231716252591931224,
                -6424990184504812975,
                3449510563345907645,
                -2315312911017493201,
                -2882693536177001662,
                7808057371513916372,
                -7557883127539867504,
                4337442397050426813,
                -4617228090736044342,
                -317809017405488560,
                -2694084398554585170,
                8391671329911424519,
                -5906045871042232771,
                8346054069554516152,
                3013428199261638328,
                -1962966499498837830,
                -6731093624132231259,
                1151851710845137329,
            ],
            vec![
                -539105096049400465,
                1965440797673655637,
                1168713912371065168,
                -4219678416479130224,
                2010201529307920846,
                3173212614489204450,
                2828695923552073550,
                5695397763888533419,
                -8857744831666494672,
                -6412479770736531306,
                -485306277026704111,
                4211599623316398638,
                3543545337981633420,
                5823163036290866790,
                6570291139551646181,
                -6483340686676889800,
                4614712777076105033,
                -4721720978938241177,
                -7211828586454977752,
                4123458707128899826,
                7844603344119790639,
                -5915382979951045498,
                -9091947581358045406,
                2185244306201390086,
                -6238874676522553532,
                3812691185278425367,
            ],
            vec![
                8407705552824841705,
                8890497784000696640,
                6781684555415854299,
                6510969233586387701,
                8053418078615116921,
                -5505087498409159540,
                -9175440181960927525,
                1662117448642332481,
                1153348233726298359,
                4910558869864262049,
                6483900086487268606,
                -6133244086053141000,
                -8134892844226059897,
            ],
            vec![
                7158213721320532427,
                4879998553216675791,
                -2639490339165371795,
                2582384446792768327,
                -8213148533832738213,
                7107838009021214566,
                3952313729938797864,
                1183563276287318031,
                -1749765136156024893,
                2808547742187867708,
            ],
            vec![
                8366923346187322167,
                -3153010071547938789,
                1398799722381329074,
                -3694136488490028317,
                -2676768672675829654,
                -3928213795442765176,
                -4719039975120951104,
                2358976222212117490,
                3271678083941359061,
                -6804466149238936420,
                -1489447640810252459,
                3831643270202847574,
                8485228976520552227,
                4844560196507493817,
                -1315142249357679214,
            ],
            vec![
                6204553297739747487,
                -669284591018315817,
                -1844638215903340416,
                -934987086001226392,
                -3427867665229128301,
                8149743880078370824,
                -4881999363878429240,
                8460038834689366267,
                1365919732332515197,
                5146833025413103891,
                5244939785804544363,
                6836841041481289174,
                7693626288438115546,
                5553969129912800343,
                -6843448088241813194,
                9156547968915937431,
                -338666500294724596,
                3607430342337873307,
                2980216905253072225,
                1426914452990598919,
                363609252184374225,
                5742392589455156456,
                5489918906875691318,
                -2827118747570602102,
                -5610310384794978047,
                1304672845979555227,
                -3926031034179642815,
                3062548861635387833,
                5893809007756179986,
                1746383024676470245,
                -4504693665257859004,
                6814024849199235775,
                -7382368453729356938,
                -5690836680802242402,
                1538773790013969251,
                8044954311573869670,
                -1625315370334423524,
                745053665916876621,
                2156349372937100973,
                6693805795308789361,
            ],
            vec![
                4261945216185996628,
                507016131595527147,
                525244923693275777,
                -7396208897011582370,
                -6149329868580124764,
                940376610832139717,
                6806074901358463597,
                -3444519396973602782,
                2837535473602382375,
                -3153828076356005425,
                668548878184898232,
                2185207892488695674,
                1295462331195726128,
                5493416298909513014,
                5705749543668774046,
                6784998890302086109,
                4599564448808887474,
                5495812061596767354,
                7109141272026892550,
                -8355809065642661986,
                -5780455051990825288,
                6116707866268364407,
                6036013667592884873,
                8971320389521188797,
                8647929842309847915,
                -4901432188852623135,
                6237049944673254124,
                -6873723031504120461,
                1843401663836082890,
                -8006691833951111683,
                -529971845095424655,
                4710122292644473372,
                1280961791706203140,
                8830446939306182313,
                -521954604074399715,
                -401017922726817928,
            ],
            vec![
                -6139203996394624561,
                5301342604954258860,
                4169437878751655444,
                -1473452717279656725,
                -9197345286013601015,
                -6502492875940549041,
                5539532840104729894,
                -3318467290603498925,
                -8319080878949291687,
                2373611456208792210,
                8670425924576339202,
                8741618223090499752,
                -1399783712176263817,
                7727268611135191436,
                8412424513207476327,
                3848863362806825587,
                3829925785063650510,
                8373764641649918728,
                6751637175110053421,
                -4124900300324132427,
                3590065281835648023,
                -1834803350471727763,
                580632878785294963,
                -8518338182751421444,
                8310527036644869394,
            ],
            vec![
                9051325311488378237,
                8422839952592803651,
                -8481484173184134392,
                8200235426340214154,
                2910025202354560142,
                3173088307755292918,
                -6520659670024304531,
                5666089926761199666,
                -2715116728026298783,
                4577665270164446201,
            ],
            vec![
                -3670492658628739574,
                -7845348549487910375,
                -1934808571881799256,
                -1770656325613932763,
                6709286677790908648,
                8846494010890992829,
                -2607089600509054294,
                6507865405767426998,
                5352675503633696772,
                7923049145998763944,
                -4337326060190357377,
                -3870991871266661614,
                1418986903554475238,
                5936994468423446793,
                -8836877900447020600,
            ],
            vec![
                4256355756616204900,
                -5314701356519127874,
                -3225308428457147149,
                611347525119970423,
                1314730912857803172,
                -6367994739596038774,
                -7795925390449693779,
                1024004613878607931,
                3558424697739743297,
            ],
            vec![
                -8306808479217776850,
                -4081517141744090373,
                3483168501677071284,
                -3381490639688293831,
                3759958419498598013,
                7652095268506559615,
                4711228681925910187,
                314874660393816064,
                -3601796889120828808,
            ],
            vec![
                1906928950639603989,
                3118826666284760372,
                2713326859317670051,
                -5236111298408368071,
                -4128828824688793900,
                -569252910650072838,
                -3941664685316992349,
                -3234929757655545638,
                -3261636715443381172,
                4702916402119365829,
                7597608936139962610,
                5273228930269244174,
                9169654774701537508,
                -4171104137841523475,
                -8969254298906029926,
                1798062998570564905,
                -5264884034813567967,
                -4609017470580742195,
                -86228909118778720,
                2192328993773901769,
                -1481259059781737787,
                -3503644419860041681,
            ],
            vec![
                -1549238272721157596,
                -4174454097265560716,
                -5016455649833847655,
                3841583602797601103,
                319063092583523490,
                -2355003323812551372,
                7548736128337500462,
                -495917980362703803,
            ],
            vec![
                -7430240601199643707,
                -8764285333538326982,
                4186099219687484665,
                5536595178303056333,
                -3930736388682152256,
                -5522736322953029198,
                -8532926294702876184,
                -418207381231054159,
                7710592661253616497,
                -511891482133875473,
                574882875784677959,
                -6545910901287381096,
                4243058694363827151,
                692931344539188946,
                5495018964104458911,
            ],
            vec![
                -3297199347037643753,
                -1063375715763420419,
                -3945431076936100941,
            ],
            vec![
                -3375431769345948331,
                2156491145008493540,
                -6369332022820631029,
                8392749529359495779,
                -809675920130494073,
                -8368399033316513860,
                -7483726224997628966,
                3910990585590451537,
                2607856729127669413,
                -1846679633020101645,
                -7928961276845469190,
                7989403863192898929,
                -3989524591366479681,
                -35337407546903982,
                7271494570777380418,
                1741830821351342452,
                -5981871171513928232,
                7016705912677045869,
                4044522010408287711,
                -1294401139636574864,
                7097413314507069375,
                -5267108080603233667,
                -7083139709646174697,
                8468760943539055071,
                1696957246393315129,
                7549446332614498491,
                655288253669138453,
                -5206412252751032220,
                5010887843809271745,
                6248230057195819764,
                -4747199555377639329,
                7149521176066045148,
                3654470864057039749,
                3149206140998492852,
                7013551938602455071,
                -6413529153182847503,
                6345515186533550407,
                -6650046838526819000,
                -9065863751907015542,
                -8611382388441774628,
                5063310743012519123,
                -1868349020432385143,
                -3732187667656357006,
                3065315015810913842,
                -6263171173505463936,
                663472665425963377,
                9104661103532712828,
            ],
            vec![
                -7686903137333323033,
                -688771348633225809,
                -1825140585767514230,
                -486101187706880079,
                4941033178786675735,
                3793880473431728749,
                -5588151963430465116,
                4001877037706432815,
                8181003819570336748,
                -1737392882164368584,
                -7494398219460282470,
                7982924388732537839,
                -7896314337459111944,
                -8857850964067928590,
                -5771664746110916701,
                -4366876777413786274,
                4384297376481098001,
                2908737601785801764,
                3707847184752639602,
                -2709553999575502989,
                1117921939085694418,
                2223234670427734702,
                -9140924696990932222,
                -6403739792136385567,
                -1431064881420816287,
                7044601872637632943,
                -2523437387500690654,
                4923184898617444028,
                294032198433426691,
                8071641763723015836,
                5144418138061242925,
                1634248228592954966,
                3216214759052844853,
                2420294815712380955,
                6174221417518474279,
                8238887606368008702,
                3910486623736512983,
                -3058464545700440201,
                351556582324456161,
                6448030216033405739,
                -4683302358721063379,
                7323927867352735196,
                2058818149277034040,
                -2125083171442280978,
                7579009924685697757,
                2585156388163446531,
                3253302186453595251,
                1088527117200972687,
                -7427659367592991957,
            ],
            vec![
                -1373011126113456595,
                251837316487261879,
                6253859576980509253,
                -4448470779581507104,
                -346907548465399229,
                4213162676592310501,
                5834666336662866837,
                5182853929847115772,
                674667728888307053,
                -8913695850566515174,
                -6004071472308162863,
                -5253131736347068292,
                -7247603821849767477,
                6687542640938877794,
                -494450834091610523,
                -2741297088876920120,
                7605865240161270961,
                6631076563860262149,
                -4502054142044792395,
                3128856650076469630,
                3770845860557119661,
            ],
            vec![
                5832018747318263696,
                6755937918159610398,
                -456259307214648840,
                6078114788716063659,
                -7582201165888621529,
                2068451483546584762,
                -2384885948276247730,
                762487537936772838,
                -8801561969764095816,
                5677459394426893496,
                3893857880020479930,
                -7412297128784364971,
                -5795057218038682237,
                -8177555849118878096,
                -2953393097564079484,
                -8362275220270664265,
                3737030041500524755,
                3071510383479309451,
                7553663876795946171,
                -5654197606830966981,
                1302056076603588611,
                -2640733679986988742,
                -2430203903835060365,
                -8420692453170200688,
                8727056901841407786,
                -1576042642499266004,
                2385511730855524533,
                1795677414784987835,
                -5110047215069655721,
                -7654237772685639450,
                -691922128072442882,
                5525556088929910453,
                -8278446090265618790,
            ],
            vec![-6362619454905854216, 1391308927327431182],
            vec![
                1307362379323366250,
                -7383435904852962605,
                2499001123034366263,
                1923017732330308647,
                -1247048502425370688,
                5366953981110303298,
                5129904687420964783,
                -1318055119719117972,
                -1547614513571052292,
                -5663008839083486720,
                903563447801857766,
                -8754966936389136568,
                1930057267922333448,
                -8797977594205544785,
                -6876986576060555088,
                -7289750728787025003,
                -2719592503436006574,
                -1066797310949309782,
                705857801761072435,
                5882840808162145921,
                -8030252175151240605,
                1648209209481052769,
                -5322175765620767174,
            ],
            vec![
                8201115745603272423,
                -5070579549252443262,
                2785554922822031722,
                -4468948752926282134,
                -1308359708415621624,
                -6183082236478695255,
                6612205536273370615,
                6102214773688738101,
                -186106684977521105,
                5613255235322783675,
                -7746686059240692939,
                6282729005794130326,
                -3626977040605528049,
                7589935915434159013,
                4680598271449263159,
                -7188738844226000784,
                6741881190524873704,
                -4312324172341067400,
                -3874542641629517198,
                -1797668029043844068,
                30403892838213259,
                5109465325437202795,
                -5863927780872598052,
                -7386369515211959578,
                1144978568154674167,
                7056757619493130450,
                3014726876739549661,
                -3778057833604333281,
                -7677127138501678248,
                -2442672028814552803,
            ],
            vec![
                -8200051236161746932,
                5228197912588080823,
                548098007568842955,
                -6757712375278297105,
                888514623912230365,
                2660511845212780414,
                -3588157103393492285,
                -5947575073726463336,
                -5566434460306114132,
                5829133991998466636,
                3695446120857347634,
                -8021229593102656092,
                -2789014197718473077,
                -4852710254763384417,
                -8833745124487179997,
                -6060664215027015198,
                4350205604149523136,
                -1842690489064236348,
                1373645340157396313,
            ],
            vec![
                7369974338491947756,
                8087641571871588211,
                -3510577244692524119,
                -4928141067255248041,
                -3302736047480288569,
                -3919559511568151335,
                742646322901108228,
                5226330836334548760,
                -8300004309015304046,
                -6836172284260948102,
                -4967410637003145030,
                7149513647586682128,
                4691021789540961143,
                -4074417501785475641,
                -5478468778131243731,
                1182334195057526933,
                -1781318393650839531,
                5579654964886285563,
                -4485959793931245916,
                -3207812147633308387,
                8779262690238499883,
                5151523216038357054,
                -6714743856599584990,
                6127232351976129468,
                6270616175834627003,
                5328673967687254637,
                -8503781900989218492,
                910999928535795451,
                4970192383393969731,
                5618144295372517161,
                -8055919794062126627,
                209974151954697195,
                -3379074877222248475,
                2894605169947069639,
                277923241835411048,
                6944159904506438026,
                -4703113327703259656,
                2911160596362521624,
                6865933797835473980,
                -4464443287784302293,
                1002704998979222966,
                2985074107117536325,
                -7732476379803397033,
                -6167267418504201934,
            ],
            vec![-8123635677344359736, -8245317105624774141],
            vec![
                3948417831330336651,
                3914561298866606817,
                -8394019662530699781,
                -1554634753238250413,
                -4430741677047309567,
                7425941923726055540,
                -1903977481599146266,
                -5033863262048977558,
                -4758121663589688998,
                6941328569845091511,
                1593120092452572723,
                7987933942194561179,
                9188551347664900228,
                -7772838159890857960,
                -4117552985600993859,
                -4908146174873162616,
                -2522390445788725639,
                -2374227373827030267,
                -6370794115288591259,
                607986217805307119,
                7942774315935228216,
                4863757055827648108,
                -203290875462729492,
                1401968401060378472,
                -5505799315834337114,
                2014944298438833850,
                7190833171387322185,
                -4256060003885339372,
                -8404574480248024230,
                436557985394546142,
                5646550497573559852,
                -8417543840244341816,
                5344458680075673131,
                3336672819960848037,
                614774711456032971,
                -18446489053396181,
                -2973007872109102617,
                -2031454920913532712,
                -5761517519078849598,
                -5892572406830931143,
                2034425308027848920,
                7877147400199226921,
                7119286022880690563,
                9063720360402169882,
                3851341209343546734,
                323805088050631393,
                3476071456026240189,
                -8282236361985050286,
            ],
            vec![
                2274957948528103977,
                6608661187390147568,
                -4480970020198644142,
                6019957889233700009,
                3819336112118208413,
                -7705999253222791261,
                8215358927977828518,
                -4905981873463186739,
                6349738174154753638,
                4077451514156866932,
                -511831231726490622,
                -699352470267582143,
                4291134553574898868,
                5051930593979808252,
                -3445526933102398803,
                3185259449906671377,
                -7843544652157347593,
                6559295595610928832,
                1688162235815707984,
                8466730236599077448,
                6117195234724293481,
                -3804811170434889599,
                -1338922548615646875,
                6248961005669059999,
                -1505506219885396324,
                -4791533770742596797,
                -8149063949062177511,
                -2363253123221752275,
                -2004896356214532515,
                6296331946651066009,
                2536800888525846247,
                209155539510852207,
                -870611173442638012,
                -4264184374799627434,
                -7980220371607675871,
                6765414612558415662,
                -4086049593607999560,
                9096125621749834232,
                -7140023808640839444,
                -4416560381208186853,
                1843644524155632134,
                4026283363938028788,
                -219662892387158236,
                5162396186351523928,
                -5708984410241773757,
            ],
            vec![
                -8481800392691459126,
                -7896166624830316154,
                -5453864015726152352,
                -8383891590645760053,
                -1964822610277064742,
                -6331311403773746847,
                4229524067568650540,
                1854706272674146274,
                -3295290091910969613,
                7871105917226195825,
                -2244215970247635416,
                67250653310947588,
            ],
            vec![
                -3448744163661967300,
                -7655064213592731184,
                4527656372457798704,
                76709372067091344,
                718201897654084010,
                1903804951875498520,
                2036897943038659294,
                1902716914997468650,
                -9096951201052128896,
                4060076758581993667,
                579003842736332303,
                1446757321926599844,
                -9199027559252226551,
                -3957001773256601999,
                4362317145069214282,
                -6582745057294442711,
            ],
            vec![
                -6464341809056998054,
                8424550008687333104,
                -3888380183565305876,
                6656601188209759050,
                -6559970152130656037,
                -1810488980399119789,
                -2433041362224239178,
                -4288277874498451784,
                6448889102016692502,
                2604206105817555621,
                4406637137646228809,
                -727066164732085142,
                -4165718041955343487,
                -8419490730741079198,
                -6904684373938089652,
                1457010914490263239,
                -1502616872731782414,
                -4674020867432567225,
                734992324918156037,
                5889165341114284678,
                -2536053500810462334,
                7470627483951219179,
                -9189738202476456318,
                -4219001457467546865,
            ],
            vec![
                -2618668654856276565,
                -4320887043217574772,
                8392810130790546249,
                3669400757006778892,
                -5751742167683229692,
                -6265765243400012543,
                -2252463766308392155,
                -7008891800521360374,
                -8840723557083041,
                2381945472490382950,
                -721751922532565758,
                -8009913270472259947,
                4086406640859434421,
                -2104171798896755424,
                7824177457461814879,
                -3027805512601028375,
                -2212576371840479427,
                -541552387256034005,
                -6690351095250509935,
                -266489060417978843,
                -7089875564319100888,
                8454284110839516366,
                -8766695162594369956,
                3657431761711431259,
                5047287669247266998,
                -4402030671067545030,
                4970762815310727396,
                -3173486421730556006,
                4185056590493891390,
                -477332692809921209,
                -5049539647134094892,
                -7744964851211926041,
                5309705376619246249,
                -5724347566359875391,
                2344248948826196691,
                502273953803305038,
                2489730825662836358,
                -4820794490682507730,
                -4794825412739099660,
                1459190998083847131,
                1840449001672250569,
                1924725772332186724,
            ],
            vec![
                45733634792238822,
                -7976287941112898424,
                4579493480341778140,
                -1153805488228124680,
                -6105407723505756438,
                -8471318566248695136,
                6534803015418730735,
                2613708954012731733,
                -561311474142889839,
                4630412938813248807,
                2858670576139706057,
                2502668754239191588,
                8773240120268147959,
                -3969147373425520192,
                -5954394868371706086,
                -1218179535173567211,
                -2429952666691268560,
                -5658569033736511273,
                -1214545647184700075,
                2722850850849875866,
                3175183762784385620,
                4429350279822202819,
                -7024029341717012546,
                391194920104820235,
                1059849165088352510,
                -1022784469699733907,
                -459980482837261269,
                690972960083038223,
                225305303825390709,
                1747374792771411675,
                -461158010314331450,
            ],
            vec![
                -1148363844259473688,
                -2048575465193094011,
                2184891530722697461,
                4123184244233130196,
                5120103905112515082,
                6045700388957265889,
                8942881749241322382,
                8136356038689108902,
                -5126916001260011128,
                5089912119424298983,
                698679289758054636,
                4434396919560383345,
                -9178057014004144272,
                3992426431403183610,
                -9148961436759983305,
                -6663014354430383442,
                -3716126965532638787,
                3808998639272352800,
                2223326883859662732,
                5638681863257684442,
                6172173089729445589,
            ],
            vec![
                -1362068352965888218,
                -4396462575734658204,
                1933290115644269626,
                1457682096420593023,
                -7084794997434743254,
                6847994396959242244,
                -3866685517053050869,
                -6489572680442547604,
                -3799564037445921727,
            ],
            vec![
                -6604383305679395624,
                3505952695067040062,
                1592654154693214220,
                576165164910399252,
                5947705355556598181,
                3935165210241129555,
                -8022385578134334339,
                -4909543585597589738,
                -7304103480238553884,
                -1096443034626554048,
                -4249307878126325749,
                8897986015517055851,
                -4178629117712279706,
                -471540408837800121,
                -3534088423698863954,
                -7398680014625166852,
                346620942209507843,
                1847578327834768785,
                -6487090711891095792,
                1564191913046509959,
                -1717414817729639516,
                2189830625317687469,
                -6032806306287940166,
                -5603116172699050396,
                -8699786348906681347,
                3706398812189545873,
                -1668516622990808449,
            ],
            vec![
                -1660583789904688778,
                -2499609229621613786,
                5052101789100543649,
                -9079250312714686091,
                6955099329014796051,
                -576164375256465362,
            ],
            vec![
                -4001031252775435076,
                7856545406816449496,
                7056086049601549877,
                -1684549935244467279,
                -2101676018799663209,
                -7927584225241990800,
                465706025717676254,
                3016379074121761469,
                9174228419990364589,
                -4562990877450423127,
                755621077978069632,
                3291547173761008480,
            ],
            vec![
                4848540699712969328,
                6745547839946080543,
                -6815465889743610736,
                -8782731822591723163,
                1213091347578382048,
                3505140257389523621,
                8754490165948549762,
                7031435343925591275,
                -6498500986675027264,
                7458940018968172762,
                -3616865389575244556,
                8551828878268294409,
                4426224085128224693,
                -6222105813236749703,
                7719130150390067340,
                -7589305017445861340,
                -558176741987235371,
                400900961243154290,
                3311461840352503815,
                -5544345735962064561,
                2074526812297599068,
                98436720858048958,
                7365313051140812024,
                -8074894000723229558,
                7171430177425733848,
                -3704256370195302199,
                3267775594026372859,
                -8880230265043004482,
                -8183078695265211109,
                8202125139165350405,
                7659787323107477665,
                -5292206566122500448,
                -4743701826938797813,
                145859211035551336,
                2569771004244449151,
                1652824828268804002,
                -2708519266333093750,
                7859026056075635933,
                8660061731760733785,
            ],
            vec![
                -8153261057632203906,
                -2799907359521971188,
                2305879543764472578,
                5030572044044140438,
                9000351240841624644,
            ],
            vec![7993289193044381138, -8360496852569206878],
            vec![
                -6077023426193670232,
                746703469009644311,
                -1731990314411856107,
                2018215057180776333,
            ],
            vec![
                -5696147587902677635,
                -3990817667642346592,
                2596991557394268218,
                -4569884629027215369,
                -8074945523237972421,
                -6869709730913977236,
                -3903467565555491614,
                4126377275026003231,
                -5518404122622128981,
                8096594660585707347,
            ],
            vec![
                -2388548999915238510,
                3799681309558327627,
                -1301349944851101365,
                2773349699758965981,
                8579262286613992661,
                -5832196864659176759,
                -3984061440544195422,
                6045965331458681595,
                -2730317157954515495,
                5008283832097623323,
                -2494987513757112872,
                -9036896127092432416,
                -3653187175886298369,
                -5738083881681416164,
                -1660683510097277240,
                -6776196321806198460,
                -8560014733101515257,
                -5859416394420452650,
                -6955134049070459940,
                3578128126546506019,
                -306800018831792607,
                4963301219217430304,
                4355494923712401608,
                -5491180374499586949,
                -6666555772832104988,
                5480239084154908700,
                -4359510386866708009,
                8098075545674001910,
                5578662472516167995,
                6993485127266516817,
                3156221689072327067,
            ],
            vec![
                -1636785253513212009,
                -1516456781965693098,
                5638600401498776658,
                856140263562791141,
                8083137330609647468,
                5820420864870400474,
                6826931953358609764,
                5447155252786389137,
                4073169564412665215,
                -4509004107004566215,
                8483595154071021763,
                1808567496814719400,
                4945413343301623325,
                9089420847555921012,
                8058920192270891233,
                7947983490391318718,
                -4661136672252743312,
                -3397106665072634680,
                -6604581814313009787,
                -965699271013951614,
                621436298981739601,
                8400416051260283094,
                -1956721127921878465,
                7019799004677089654,
                3921238073494040771,
                502117409297279561,
                7732508050728031969,
                -2599371314773791384,
                -2795729123929049873,
                5825825613105783796,
                7564799402337498722,
                -47084193644258705,
                1523134952670776495,
                -1917664831448728442,
                -7357303535243823716,
                576573635612047402,
                3659737128337849419,
                -2707027205932089950,
            ],
            vec![
                915514144910761026,
                6364587503442283306,
                6854814367345658333,
                -6863603312793565620,
                -4995849302037250295,
                7052402332041344923,
                -7567437657144020922,
                2353907247092065368,
                4514503508096078556,
                2904628366985507653,
                -3397144575394730139,
                1591118790736663553,
                -7434425595410342622,
                -6982477313275153203,
                -3491856593144849567,
                -46888171932981895,
                6238106828846676621,
                -6910023174981059933,
                -3305326392375845879,
                5448393733908165790,
                -7748218510303179144,
                -6903084222186817134,
                5577479043172775324,
                4996236155811496175,
                -3194601796337691488,
                -5608463972038728159,
                -6674231864509309813,
                1938759800354789398,
                -3584009392063185045,
                3920643897033594490,
                255738335592712650,
                4958760372919464731,
                4999220684868256266,
                1234365940642097996,
                -479634276256406914,
                -4795567196545933393,
                1810746626166748706,
                -4147947864511493204,
                6199150610018412634,
                -3973686291296178225,
                -7580798990079421427,
            ],
            vec![
                -7430332697935299586,
                92816732138666175,
                -4660585054003086739,
                5674184885818751883,
                6352368012152511466,
                -8271077844142383234,
                5758148901215123533,
            ],
            vec![
                -2340839589418851879,
                -2007594996408313561,
                -4972035879000448497,
                1128868751627248475,
                -7007592143617724893,
                1468899257608001998,
                -6000211325370497450,
                -8025350420320028776,
                9108417879729432602,
                -534700972713309113,
                3056386933283820414,
                5825961706873587517,
                -6518202876590139806,
                912629574090081777,
                -8662178825539389945,
                7997652306828773314,
                5909808407533347867,
                8899795035361327059,
                -636182629692059917,
                -6392502091182112550,
                -476522321523098524,
                7989422928533946687,
                5600647482800472919,
            ],
            vec![
                -8535530660748460715,
                2037220198882141627,
                -7194806975129083499,
                -1574512969511247350,
                -504626446109637238,
                -4989045393571416267,
                3193473339699500809,
                6613888387093532785,
                -5731056747805407761,
                -342913806909869550,
                2535670224618174844,
                -7741228158682568067,
                -8287240709486391672,
                -3769160144607708468,
                4738120872772116,
                5978432987157557940,
                -3880416820144919516,
                2737632722847888387,
                7632922784403529702,
                8962432232296147842,
                157107376636796274,
                2747231357073539029,
                -7352215614412142541,
                -8970872604751017740,
                9150249802921250636,
                978890034461755074,
                -9000660050623652810,
                7510402580718974805,
                416974215074430244,
                -873576578486083540,
                7459906955690892479,
                -5600954131516955493,
                -3718856132471345347,
                -657956719892629156,
                5259638033903857593,
                8708099662877256008,
                -599183572400785608,
            ],
            vec![
                4009882776523321223,
                3686293509775855071,
                8056604416339069009,
                -3511207112055044797,
                -4382248389238228295,
                556549600145063594,
                4870365108870333970,
                -5307000712737419756,
                5856916516302341406,
                2026958261759548010,
                -3878900422736753931,
                2891506412039241055,
                -3258384929874326848,
                -5363867830558553360,
                -1243918259413754169,
                -8520722219505415381,
                -7069086655802808239,
                -778070577655308672,
            ],
            vec![
                -1058038427663714172,
                3695759641859120858,
                -3380656682392896194,
                -1025697315672769899,
                8871202138647294043,
                -5354767505838087270,
            ],
            vec![
                -2921933543524971190,
                -1663942673705253682,
                2949268394518243463,
                7340323699554511516,
                -8670987640485400992,
                -4363579940073747217,
                -1472346557063627217,
                -5587501058139231813,
                -6017611671347409123,
                2001278905797272605,
                495909084206167782,
                4636198237082921888,
                8845563383250553438,
                -7833008753372414637,
                -7311381775610450292,
                6385983162519202655,
                -4302212366460105366,
                6222622436715025439,
                1321588061873409419,
                -2389916475486703225,
                1101141719837271924,
                -8096356915451203267,
                2639373693182925025,
                1004162758921531914,
                6803900166303371698,
                -4855860083317986877,
                1321196703041772993,
                2550110703425357591,
                -6296201103213864560,
                -6872395451735467134,
                4760517845056351387,
                -6298336580177048226,
                -4284882466011758231,
                -8451031145739046925,
            ],
            vec![
                -7742382528382071308,
                3100904392485946935,
                -2845353772103726320,
                7270903456513971717,
                -165327115437642727,
                6388643988142929040,
                -5466958616126385382,
                913019058443146579,
                4092205065968462028,
                3941474141535796917,
                5619061819217350871,
                3715453105851753479,
                1472430010985297779,
                1865750889741557984,
            ],
            vec![
                -1935281189874585599,
                3420183458241154805,
                -2442327733376291526,
                -3681203289463843785,
                3200015503744975593,
                -1659068446491071154,
                5472284964434312017,
                -7638387537843426279,
                -1223159233340417384,
                6125732667909718186,
                -8975908118607875254,
                -2181731302661280944,
                8711369372041740318,
                6799943292169482275,
                -5361556872024341127,
                1768226388644794915,
                -7062867463929950963,
                -3119222707071965162,
                -7833056479067710035,
                2044950535540050162,
                3135224582336495141,
                -3337058381848700139,
                1121024051389031239,
                5332088920148254716,
                981083942269127317,
                1127611584632068914,
                8740361191731558965,
                -3463125933153312187,
                4947977677292870328,
                7189577731704709014,
                -2540056843867815248,
                -2040080282302244237,
                3785610771416300415,
                -6653257019185217079,
                -5212781892294979346,
            ],
            vec![
                8432946437869268540,
                -6003011606226077571,
                -7365908243099687131,
                -8675370435979744048,
                692420207113100243,
                4652548612571603107,
                8763921922422839372,
                -850059524890830776,
                -8731226789026295717,
                5821326036524776380,
                6829944983085865439,
                4600738452758600383,
                1833789231063887059,
                -482782285052855734,
                -4114645737430844149,
                4314914644985004150,
                2035398182616012967,
                -2701908696275788374,
                1709916441190912940,
                -6513900463219098730,
                -3666816972987463844,
                4345368385340565647,
                5154568882812475532,
                8218809674113283000,
                3794735817471263799,
                -32236881216357153,
                2104277940305672350,
                -7483156258680634240,
                2491543807436824445,
                -8105723079654791058,
                -3795124082226447252,
                -8202683498293636388,
                2069562236016150323,
                6800136112289931418,
                9083634259845564699,
                -5206867000944494196,
                -2849206814906616135,
                7365572366244301106,
                7754848324950185589,
                371961597379631444,
                -819092188308261331,
                -2011906803921323447,
                -4808176451102970352,
                2311859236119922098,
                -5502597262569677442,
            ],
            vec![
                -6915901028145506372,
                -4951243176179725366,
                -3215598822822006537,
                -5373353842278080595,
                2915843339780316006,
                -800249846247072851,
                -2335946940335766916,
                -4919139929970158951,
                7770604229695428933,
                -5663632705268522402,
                4390805190130724662,
                6417220423093547206,
                -6192322834066621243,
                662932183187263528,
                2170122633472995856,
                -5987668273483742778,
                2265239201182171862,
                -7448858366401451566,
                4315428973136232387,
                -344638228447766090,
                -8117767006025677417,
                8677075872276707683,
                -4573800331114052722,
                6810145379841066596,
                -246483033977058084,
                -982491108957880713,
                3185747175412376386,
                -5627643158561930666,
                8550002227172371673,
                -225342762492617311,
                4542965906172345019,
                822627745583043764,
                3193577732301509348,
                -3909979706362630837,
                -3460730357821614467,
                -1905463689069833386,
                2691376855132361235,
                -1982251148988189328,
                8977156009731050998,
                2545738951084904743,
                1741217476602791474,
                3663097109207950300,
                3496941192655040954,
                1676206881153673871,
                -5257645196831384243,
            ],
            vec![
                -2525851922717610390,
                -8981472097220661511,
                -4379749057100253484,
                -8424975304550261258,
                4423924061883938690,
                -507616342184889010,
                3937290086200925138,
                9097012683769339363,
                -2016435719965396602,
                -2590090406510065547,
                407116829094682717,
                3429238659619566776,
                5555305898467436419,
                -8104383124267252874,
                -201172962858213897,
                6080612367128404227,
                -2747277344553788540,
                2514834546027786409,
                -864389388376922418,
                -6784691937008118649,
                -6177245990229019111,
                -207894714699873579,
                -769042855765596642,
                -8879503904464772490,
                -4499661369100957559,
                -5038213300759604947,
                -1421739275871385154,
                -3650008743253654530,
                -6272264554681713539,
                5123314789990551710,
                -2153461076393363064,
                2166248561749164860,
                7346925575501387876,
                5527589532220592726,
                769735979314949341,
                -2352492306332200,
                -2499314567908579909,
                -1627321150983006921,
                1819062974354433125,
                -4271865749721560630,
                1497535194762381019,
                4460395996950361568,
                -42749758106654110,
                -8241507074694152850,
                6701295987988944724,
                4584231601214550256,
                4824627498929433260,
            ],
            vec![
                -5733685843400806749,
                -4279018273887291179,
                -6870602887400672041,
                -404204097506093565,
                7377644295886787836,
                -4493545038272436484,
                -6468203850511826152,
                -523254150520153021,
                -2990257338963765118,
                3491689154714192720,
                6292223045545407594,
                -2012978451616838316,
                -6532145612222594813,
                6245361308506192404,
                -2878103280667079627,
                -543672369686959727,
                3771393559929573663,
                -1764391307813154953,
                9099294582982076243,
                460665592687566902,
                6538443042958493126,
                3549902293686059954,
                4377266469858893294,
                8281909720164459718,
                -1763832319283633122,
                -4843406399552559745,
                3354315868980110929,
                3722001264610860282,
                -7355576177930513767,
                8041808047025038867,
                6150142505992678206,
                1241203482375761151,
                8937206800996506944,
                5684496819366995534,
                2772488208130555542,
                -6370834260673765297,
                -3430810100081116398,
                -6370407366044753603,
                3656326341905074673,
                -4021329198745506875,
            ],
            vec![
                -5453201794900862447,
                8403073036886271980,
                -4741824281205408686,
                -5766543295789904773,
                -3131440621978424538,
                7279410415337345383,
                3250239546204474399,
                -8451298125609021246,
                1587200041552355245,
                2943861123819853312,
                -6737697524841131366,
                -2905897843653154194,
                -8557141554498370471,
                1260493338820010431,
                8562446885309091624,
                -4488293774645274999,
                -3303654953239753945,
                -468528967481508791,
                2241180077571157814,
                -4327056058305374575,
                -5718727620917236072,
                6962148327429601907,
                1645492989096492815,
                -8288956426753396622,
                -6700797516260924920,
                -7299460061084292963,
                -6202961760401281960,
                -9192799950077757315,
                -1609934184943400724,
                -8544595299270963851,
                2625271048591091992,
                -5327400492155786998,
                -2212891720657090961,
                -4848569311199809261,
                -5949580161004356717,
                -5024817123900698349,
                -1726572866834744405,
                -4734144667130538267,
                -6002297283741397747,
                -6261200520421359456,
                -6283958959618282430,
                5101245372628429912,
                -5598881956420734412,
                861045040085819553,
                5602386833523163268,
                -5921220844833267335,
                4728060198238240262,
            ],
            vec![
                5965120847543690228,
                -4987045246746224063,
                -7550170603158958648,
                5042897443960885509,
                -7920225044250405747,
                -2528601241790137297,
                -8724447777151544136,
                -5780197499270709320,
                -3619831125119052878,
                1318507439441491596,
                8076832115091269290,
                -3215497263746167531,
                6216394577866737062,
                7515159973499293435,
                909294086983888890,
                -6882766459865356647,
                2068387940420591096,
                5256924956002876832,
                -4127367418894905926,
                4563816537284799599,
                1895903482298129582,
                -1982788530422240888,
                -4148465524445959079,
                5273366646034318536,
                1691622702018598799,
                3912212636853626113,
                4115186607966814317,
                -8942853519201328692,
            ],
            vec![
                5214770496419594270,
                5547248136517255932,
                -890016893088494987,
                8700214092659194657,
                -6461693432813792426,
                8182184449744323594,
                -3062224971910157897,
                8259439517645374143,
            ],
            vec![
                -465530031570216180,
                -1941643561444195279,
                -7295820251152439177,
                3718593053801104979,
                1958648285130754502,
                7608783023176881743,
                -6746022041813910217,
                3563502202440469563,
                2897757135344653376,
                -473245296814393643,
                8352207530220577908,
                -639657953550897341,
                4020220095484041559,
                -704200313855279440,
                -6161455784781846566,
                7474150864793580382,
                4121425716752771689,
                -5986979564875315300,
                -7952893110884930772,
                5195001079008543260,
                6178837278646503561,
                1515007285463032962,
                4341277726380485545,
                2328663347768352064,
            ],
            vec![
                -50035875894576174,
                4326472220056917294,
                -4015128467228641231,
                6019012675992466192,
                2141786247919612768,
                4541439286480908110,
                -1764227195628213260,
                4122492997486448683,
                -4419684676222588652,
                -3204603780465853190,
                6350203326412413966,
                -1198132524832179034,
                5674175946388699859,
                -6005383287221822098,
                -69573618624045818,
                5180166694290073085,
                -1080489725918490965,
                -8367610937528745116,
                -2984577046879543163,
                -3873505967536487571,
            ],
            vec![
                2980589121250066296,
                -3313256748772424812,
                -2034826751344004371,
                -7555622550975636857,
                7111949946480697415,
            ],
            vec![
                -869935696686824777,
                -7681560334134240861,
                1792685009275616786,
                -7868341356035193467,
                -6121052787312116410,
                -4453437022133371220,
            ],
            vec![
                -6579823440067436191,
                994407907889230473,
                4223096635339513957,
                -1468611204289727988,
            ],
            vec![
                -2614194707520115224,
                356132164355470921,
                7133423852690426160,
                -1632173349771341170,
                -3225941173317415087,
                4331329934475995062,
                -8651076016418410529,
                -4704148615314474099,
                -19691460351675507,
                -4191005728916675938,
                2419684796152464095,
                -8480377004796157953,
                3886680766732010312,
                -2619276362471092722,
                1669290393446683288,
                10957242936845992,
                -9040601582974388802,
                7857794568117030157,
                -8083357302666806123,
                7649559698941653528,
                8640732985087898485,
                163189858050034378,
            ],
            vec![
                6578131465001188377,
                4499944351947080635,
                2963162194313433997,
                -380167216422579606,
                -7842084930353755640,
                -8757543106715441424,
                -998039563044793012,
                99834527582406274,
                -4928374933159335916,
                -6223344467845335857,
                -7275987217172793071,
                5874564128542672704,
                7508550848567011640,
                2640049947576836170,
                -4503737231056220487,
                4926041471181218251,
                7224891708543142959,
                1337918771427133754,
                6301283559840289490,
                3809389893140387194,
                4974708995827433175,
                360032635450424012,
                4952373612290343271,
                3637641067018707977,
                2985057414198475026,
                375400672753833505,
                -8706394578243297970,
                -5454304550485878490,
                -768014930105167716,
                51124756533838608,
                -420104433565835343,
                677568377116639222,
                -7071498027493439199,
                -5764892157802846937,
                -1827101572423427567,
                6379645047464573135,
                -2684306053667539579,
                3092764232224693402,
                -745380376268146877,
                8777334943633107938,
                6714433580727215263,
                8858857401905845930,
            ],
            vec![
                3855068982356538309,
                5480701360287610831,
                7007842808161625962,
                -6912375667287392133,
                -5804640960284425788,
                3821072582925158869,
                -5929155535948746019,
            ],
            vec![7075014237935156343],
            vec![-3237351554444345598, 4596773026976670607],
            vec![
                6522991401569339654,
                1240618106866248215,
                -6065180646617081464,
                -5477947507808094099,
                679742099032173552,
                -1871382202532415775,
                830161843136004431,
            ],
            vec![
                9157631838891181739,
                -4657457224345077446,
                -4671202015031646225,
                3208893271054647516,
                -481997136581985505,
                -1288575944534013833,
                -3051622190980675694,
                4484265763551657217,
                6338178040644858579,
                -7538688334670166366,
                5130512516131792156,
                -8322982501627466487,
            ],
            vec![
                -3589300589512351840,
                6650419445196884195,
                4197603453539641247,
            ],
            vec![],
            vec![
                -9035965601917843423,
                -8771424462901358015,
                -3374507141944557747,
            ],
        ];

        for values in test_data {
            // Write values to encoder.
            let mut enc = IntegerEncoder::new(1024);
            for v in &values {
                enc.write(*v);
            }

            // Retrieve encoded bytes from encoder.
            let buf = enc.bytes().unwrap();

            // Read values out of decoder.
            let mut got = Vec::with_capacity(values.len());
            let mut dec = IntegerDecoder::new(buf.as_slice()).unwrap();
            while dec.next() {
                assert_eq!(dec.err().is_none(), true);
                got.push(dec.read());
            }

            assert_eq!(got, values);
        }
    }

    // #[test]
    // fn test_integer_decoder_corrupt() {
    //     let cases = [
    //         "",                     // Empty
    //         "\x00abc",              // Uncompressed: less than 8 bytes
    //         "\x10abc",              // Packed: less than 8 bytes
    //         "\x20abc",              // RLE: less than 8 bytes
    //         "\x2012345678\x90",     // RLE: valid starting value but invalid delta value
    //         "\x2012345678\x01\x90", // RLE: valid starting, valid delta value, invalid repeat value
    //     ];
    // }
}
