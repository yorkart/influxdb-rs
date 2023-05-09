//! Timestamp encoding is adaptive and based on structure of the timestamps that are encoded.  It
//! uses a combination of delta encoding, scaling and compression using simple8b, run length encoding
//! as well as falling back to no compression if needed.
//!
//! Timestamp values to be encoded should be sorted before encoding.  When encoded, the values are
//! first delta-encoded.  The first value is the starting timestamp, subsequent values are the difference
//! from the prior value.
//!
//! Timestamp resolution can also be in the nanosecond.  Many timestamps are monotonically increasing
//! and fall on even boundaries of time such as every 10s.  When the timestamps have this structure,
//! they are scaled by the largest common divisor that is also a factor of 10.  This has the effect
//! of converting very large integer deltas into very small one that can be reversed by multiplying them
//! by the scaling factor.
//!
//! Using these adjusted values, if all the deltas are the same, the time range is stored using run
//! length encoding.  If run length encoding is not possible and all values are less than 1 << 60 - 1
//! (~36.5 yrs in nanosecond resolution), then the timestamps are encoded using simple8b encoding.  If
//! any value exceeds the maximum values, the deltas are stored uncompressed using 8b each.
//!
//! Each compressed byte slice has a 1 byte header indicating the compression type.  The 4 high bits
//! indicate the encoding type.  The 4 low bits are used by the encoding type.
//!
//! For run-length encoding, the 4 low bits store the log10 of the scaling factor.  The next 8 bytes are
//! the starting timestamp, next 1-10 bytes is the delta value using variable-length encoding, finally the
//! next 1-10 bytes is the count of values.
//!
//! For simple8b encoding, the 4 low bits store the log10 of the scaling factor.  The next 8 bytes is the
//! first delta value stored uncompressed, the remaining bytes are 64bit words containing compressed delta
//! values.
//!
//! For uncompressed encoding, the delta values are stored using 8 bytes each.

use bytes::BufMut;

use crate::engine::tsm1::codec::varint::VarInt;
use crate::engine::tsm1::codec::{simple8b, Decoder, Encoder};

/// TIME_UNCOMPRESSED is an uncompressed format using 8 bytes per timestamp
const TIME_UNCOMPRESSED: u8 = 0;
/// TIME_COMPRESSED_PACKED_SIMPLE is a bit-packed format using simple8b encoding
const TIME_COMPRESSED_PACKED_SIMPLE: u8 = 1;
/// TIME_COMPRESSED_RLE is a run-length encoding format
const TIME_COMPRESSED_RLE: u8 = 2;

/// TimeEncoder encodes time.Time to byte slices.
pub struct TimeEncoder {
    ts: Vec<u64>,
    enc: simple8b::Encoder,
}

impl TimeEncoder {
    pub fn new(sz: usize) -> Self {
        Self {
            ts: Vec::with_capacity(sz),
            enc: simple8b::Encoder::new(),
        }
    }

    fn reduce(&mut self) -> (u64, u64, bool) {
        // Compute the deltas in place to avoid allocating another slice
        let deltas = self.ts.as_mut_slice();

        // Starting values for a max and divisor
        let mut max = 0_u64;
        let mut divisor = 1000000000000_u64; // 1e12

        // Indicates whether the deltas can be run-length encoded
        let mut rle = true;

        // Iterate in reverse so we can apply deltas in place
        for i in (1..deltas.len()).rev() {
            // First differential encode the values
            (deltas[i], _) = deltas[i].overflowing_sub(deltas[i - 1]);

            // We also need to keep track of the max value and largest common divisor
            let v = deltas[i];

            if v > max {
                max = v;
            }

            // If our value is divisible by 10, break.  Otherwise, try the next smallest divisor.
            while divisor > 1 && v % divisor != 0 {
                divisor /= 10;
            }

            // Skip the first value || see if prev = curr.  The deltas can be RLE if there are all equal.
            rle = i == deltas.len() - 1 || rle && (deltas[i + 1] == deltas[i])
        }

        return (max, divisor, rle);
    }

    fn encode_packed(&mut self, div: u64) -> anyhow::Result<Vec<u8>> {
        // Only apply the divisor if it's greater than 1 since division is expensive.
        if div > 1 {
            for v in &self.ts[1..] {
                self.enc.write(*v / div)?;
            }
        } else {
            for v in &self.ts[1..] {
                self.enc.write(*v)?;
            }
        }

        // The compressed deltas
        let deltas = self.enc.bytes()?;

        let sz = 8 + 1 + deltas.len();
        let mut bytes = Vec::with_capacity(sz);

        let b0 = {
            // 4 high bits used for the encoding type
            let mut b0 = (TIME_COMPRESSED_PACKED_SIMPLE as u8) << 4;
            // 4 low bits are the log10 divisor
            b0 |= ((div as f64).log10()) as u8;
            b0
        };
        bytes.push(b0);

        // The first delta value
        bytes.put_u64(self.ts[0]);

        bytes.extend_from_slice(deltas);

        Ok(bytes)
    }

    fn encode_raw(&mut self) -> anyhow::Result<Vec<u8>> {
        let sz = 1 + self.ts.len() * 8;
        let mut bytes = Vec::with_capacity(sz);

        bytes.push((TIME_UNCOMPRESSED as u8) << 4);
        for v in &self.ts {
            bytes.put_u64(*v as u64);
        }

        Ok(bytes)
    }

    fn encode_rle(&mut self, first: u64, delta: u64, div: u64) -> anyhow::Result<Vec<u8>> {
        // Large varints can take up to 10 bytes, we're encoding 3 + 1 byte type
        let mut bytes = Vec::with_capacity(31);

        let b0 = {
            // 4 high bits used for the encoding type
            let mut b0 = (TIME_COMPRESSED_RLE as u8) << 4;
            // 4 low bits are the log10 divisor
            b0 |= ((div as f64).log10()) as u8;
            b0
        };
        bytes.push(b0);

        let mut tmp = [0u8; 10];

        // The first timestamp
        bytes.put_u64(first);
        // The first delta
        let mut sz = ((delta / div) as u64).encode_var(&mut tmp);
        bytes.extend_from_slice(&tmp[..sz]);

        // The number of times the delta is repeated
        sz = (self.ts.len() as u64).encode_var(&mut tmp);
        bytes.extend_from_slice(&tmp[..sz]);

        Ok(bytes)
    }
}

impl Encoder<i64> for TimeEncoder {
    fn write(&mut self, v: i64) {
        self.ts.push(v as u64);
    }

    fn flush(&mut self) {}

    fn bytes(&mut self) -> anyhow::Result<Vec<u8>> {
        if self.ts.len() == 0 {
            return Ok(vec![]);
        }

        // Maximum and largest common divisor.  rle is true if dts (the delta timestamps),
        // are all the same.
        let (max, div, rle) = self.reduce();

        // The deltas are all the same, so we can run-length encode them
        if rle && self.ts.len() > 1 {
            return self.encode_rle(self.ts[0], self.ts[1], div);
        }

        // We can't compress this time-range, the deltas exceed 1 << 60
        if max > simple8b::MAX_VALUE {
            return self.encode_raw();
        }

        return self.encode_packed(div);
    }
}

pub enum TimeDecoder<'a> {
    RleDecoder(RleDecoder),
    PackedDecoder(PackedDecoder<'a>),
    UncompressedDecoder(UncompressedDecoder<'a>),
    EmptyDecoder(EmptyDecoder),
}

impl<'a> TimeDecoder<'a> {
    pub fn new(b: &'a [u8]) -> anyhow::Result<Self> {
        if b.len() > 0 {
            let encoding = b[0] >> 4;
            // Lower 4 bits hold the 10 based exponent, so we can scale the values back up
            let div = u64::pow(10, (b[0] & 0xF) as u32);

            let b = &b[1..];
            match encoding {
                TIME_UNCOMPRESSED => Ok(TimeDecoder::UncompressedDecoder(
                    UncompressedDecoder::new(b)?,
                )),
                TIME_COMPRESSED_PACKED_SIMPLE => {
                    Ok(TimeDecoder::PackedDecoder(PackedDecoder::new(b, div)?))
                }
                TIME_COMPRESSED_RLE => Ok(TimeDecoder::RleDecoder(RleDecoder::new(b, div)?)),
                _ => Err(anyhow!("unknown encoding {}", encoding)),
            }
        } else {
            Ok(TimeDecoder::EmptyDecoder(EmptyDecoder {}))
        }
    }
}

impl<'a> Decoder<i64> for TimeDecoder<'a> {
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

impl<'a> Decoder<i64> for EmptyDecoder {
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

impl RleDecoder {
    pub fn new(bytes: &[u8], div: u64) -> anyhow::Result<Self> {
        if bytes.len() == 0 {
            return Err(anyhow!(
                "TimeDecoder: empty data to decode RLE starting value"
            ));
        }
        if bytes.len() < 8 {
            return Err(anyhow!(
                "TimeDecoder: not enough data to decode RLE starting value"
            ));
        }

        let mut i = 0;

        // Next 8 bytes is the starting timestamp
        let first = u64::from_be_bytes(bytes[i..i + 8].try_into().unwrap());
        i += 8;

        // Next 1-10 bytes is the delta value
        let (mut delta, n) = u64::decode_var(&bytes[i..])
            .ok_or(anyhow!("TimeDecoder: invalid run length in decodeRLE"))?;
        delta *= div;
        i += n;

        // Last 1-10 bytes is how many times the value repeats
        let (repeat, _n) = u64::decode_var(&bytes[i..])
            .ok_or(anyhow!("TimeDecoder: invalid repeat value in decodeRLE"))?;

        Ok(Self {
            first: first as i64,
            delta: delta as i64,
            repeat,
            step: -1,
        })
    }
}

impl<'a> Decoder<i64> for RleDecoder {
    fn next(&mut self) -> bool {
        self.step += 1;

        if self.step >= self.repeat as i64 {
            return false;
        }

        if self.step > 0 {
            (self.first, _) = self.first.overflowing_add(self.delta);
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
    div: u64,

    bytes: &'a [u8],
    b_step: usize,

    values: [u64; 240],
    v_step: usize,
    v_len: usize,

    err: Option<anyhow::Error>,
}

impl<'a> PackedDecoder<'a> {
    pub fn new(bytes: &'a [u8], div: u64) -> anyhow::Result<Self> {
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
        let first = u64::from_be_bytes(bytes[0..8].try_into().unwrap());

        Ok(Self {
            first: first as i64,
            div,
            bytes,
            b_step: 0,
            values: [0; 240],
            v_step: 0,
            v_len: 0,
            err: None,
        })
    }
}

impl<'a> Decoder<i64> for PackedDecoder<'a> {
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
            self.first += (self.values[self.v_step] * self.div) as i64;
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
        let r = simple8b::decode(self.values.as_mut(), v);
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

        self.first += (self.values[self.v_step] * self.div) as i64;
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
            first: first as i64,
            bytes,
            b_step: 0,
            err: None,
        })
    }
}

impl<'a> Decoder<i64> for UncompressedDecoder<'a> {
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
        (self.first, _) = self.first.overflowing_add(v as i64);
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

pub fn count_timestamps(b: &[u8]) -> anyhow::Result<usize> {
    if b.len() == 0 {
        return Err(anyhow!("count_timestamps: no data found"));
    }

    // Encoding type is stored in the 4 high bits of the first byte
    let encoding = b[0] >> 4;
    match encoding {
        TIME_UNCOMPRESSED => {
            // Uncompressed timestamps are just 8 bytes each
            Ok((b.len() - 1) / 8)
        }
        TIME_COMPRESSED_RLE => {
            // First 9 bytes are the starting timestamp and scaling factor, skip over them
            let mut i = 9;
            // Next 1-10 bytes is our (scaled down by factor of 10) run length values
            let (_, n) = u64::decode_var(&b[i..])
                .ok_or(anyhow!("count_timestamps: can not decode delta"))?;
            i += n;
            // Last 1-10 bytes is how many times the value repeats
            let (count, _) = u64::decode_var(&b[i..])
                .ok_or(anyhow!("count_timestamps: can not decode repeat"))?;

            Ok(count as usize)
        }
        TIME_COMPRESSED_PACKED_SIMPLE => {
            // First 9 bytes are the starting timestamp and scaling factor, skip over them
            let count = simple8b::count_bytes(&b[9..])?;
            // +1 is for the first uncompressed timestamp, starting timestamp in b[1:9]
            Ok(count + 1)
        }
        _ => Err(anyhow!(
            "count_timestamps: unsupported encoding {}",
            encoding
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Add;
    use std::time::Duration;

    use influxdb_utils::time;

    use crate::engine::tsm1::codec::timestamp::{
        Decoder, TimeDecoder, TimeEncoder, TIME_COMPRESSED_PACKED_SIMPLE, TIME_COMPRESSED_RLE,
        TIME_UNCOMPRESSED,
    };
    use crate::engine::tsm1::codec::Encoder;

    #[test]
    fn test_time_encoder() {
        let mut enc = TimeEncoder::new(1);

        let mut x = Vec::new();
        let now = Duration::new(0, 0);
        x.push(now.as_nanos() as i64);
        enc.write(now.as_nanos() as i64);
        for i in 1..4_usize {
            x.push(now.add(Duration::from_secs(i as u64)).as_nanos() as i64);
            enc.write(x[i]);
        }

        let b = enc.bytes().unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            got, TIME_COMPRESSED_RLE,
            "Wrong encoding used: expected rle, got {}",
            got
        );

        let mut dec = TimeDecoder::new(b.as_slice()).unwrap();
        for (i, v) in x.iter().enumerate() {
            assert_eq!(dec.next(), true, "Next == false, expected true");
            assert_eq!(
                dec.read(),
                *v as i64,
                "Item {} mismatch, got {}, exp {}",
                i,
                dec.read(),
                *v as i64
            )
        }
    }

    #[test]
    fn test_time_encoder_no_values() {
        let mut enc = TimeEncoder::new(0);
        let b = enc.bytes().unwrap();

        let mut dec = TimeDecoder::new(b.as_slice()).unwrap();
        assert_eq!(
            dec.next(),
            false,
            "unexpected next value: got true, exp false"
        );
    }

    #[test]
    fn test_time_encoder_one() {
        let mut enc = TimeEncoder::new(1);
        let tm = 0_i64;

        enc.write(tm);
        let b = enc.bytes().unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            got, TIME_COMPRESSED_PACKED_SIMPLE,
            "Wrong encoding used: expected packed_simple, got {}",
            got
        );

        let mut dec = TimeDecoder::new(b.as_slice()).unwrap();
        assert_eq!(
            dec.next(),
            true,
            "unexpected next value: got true, exp false"
        );
        assert_eq!(
            dec.read(),
            tm,
            "read value mismatch: got {}, exp {}",
            dec.read(),
            tm
        );
    }

    #[test]
    fn test_time_encoder_two() {
        let mut enc = TimeEncoder::new(2);

        let t0 = 0_i64;
        let t1 = 1_i64;

        enc.write(t0);
        enc.write(t1);

        let b = enc.bytes().unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            got, TIME_COMPRESSED_RLE,
            "Wrong encoding used: expected packed_simple, got {}",
            got
        );

        let mut dec = TimeDecoder::new(b.as_slice()).unwrap();
        assert_eq!(
            dec.next(),
            true,
            "[t0]unexpected next value: got true, exp false"
        );
        assert_eq!(
            dec.read(),
            t0,
            "read value mismatch: got {}, exp {}",
            dec.read(),
            t0
        );

        assert_eq!(
            dec.next(),
            true,
            "[t1]unexpected next value: got true, exp false"
        );
        assert_eq!(
            dec.read(),
            t1,
            "read value mismatch: got {}, exp {}",
            dec.read(),
            t1
        );

        assert_eq!(
            dec.next(),
            false,
            "unexpected next value: got true, exp false"
        );
    }

    #[test]
    fn test_time_encoder_three() {
        let mut enc = TimeEncoder::new(3);

        let t1 = 0_i64;
        let t2 = 1_i64;
        let t3 = 3_i64;

        enc.write(t1);
        enc.write(t2);
        enc.write(t3);

        let b = enc.bytes().unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            got, TIME_COMPRESSED_PACKED_SIMPLE,
            "Wrong encoding used: expected packed_simple, got {}",
            got
        );

        let mut dec = TimeDecoder::new(b.as_slice()).unwrap();
        assert_eq!(
            dec.next(),
            true,
            "[t1]unexpected next value: got true, exp false"
        );
        assert_eq!(
            dec.read(),
            t1,
            "read value mismatch: got {}, exp {}",
            dec.read(),
            t1
        );

        assert_eq!(
            dec.next(),
            true,
            "[t2]unexpected next value: got true, exp false"
        );
        assert_eq!(
            dec.read(),
            t2,
            "read value mismatch: got {}, exp {}",
            dec.read(),
            t2
        );

        assert_eq!(
            dec.next(),
            true,
            "[t3]unexpected next value: got true, exp false"
        );
        assert_eq!(
            dec.read(),
            t3,
            "read value mismatch: got {}, exp {}",
            dec.read(),
            t3
        );

        assert_eq!(
            dec.next(),
            false,
            "unexpected next value: got true, exp false"
        );
    }

    #[test]
    fn test_time_encoder_large_range() {
        let mut enc = TimeEncoder::new(2);

        let t0 = 1442369134000000000_i64;
        let t1 = 1442369135000000000_i64;

        enc.write(t0);
        enc.write(t1);

        let b = enc.bytes().unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            got, TIME_COMPRESSED_RLE,
            "Wrong encoding used: expected packed_simple, got {}",
            got
        );

        let mut dec = TimeDecoder::new(b.as_slice()).unwrap();
        assert_eq!(
            dec.next(),
            true,
            "[t0]unexpected next value: got true, exp false"
        );
        assert_eq!(
            dec.read(),
            t0,
            "read value mismatch: got {}, exp {}",
            dec.read(),
            t0
        );

        assert_eq!(
            dec.next(),
            true,
            "[t1]unexpected next value: got true, exp false"
        );
        assert_eq!(
            dec.read(),
            t1,
            "read value mismatch: got {}, exp {}",
            dec.read(),
            t1
        );

        assert_eq!(
            dec.next(),
            false,
            "unexpected next value: got true, exp false"
        );
    }

    #[test]
    fn test_time_encoder_uncompressed() {
        let mut enc = TimeEncoder::new(3);

        let t1 = Duration::new(0, 0).as_nanos() as i64;
        let t2 = Duration::new(1, 0).as_nanos() as i64;

        // about 36.5yrs in NS resolution is max range for compressed format
        // This should cause the encoding to fallback to raw points
        let t3 = 1152921506606846976_i64; // Duration::new(2, 2 << 59).as_nanos() as u64;

        enc.write(t1);
        enc.write(t2);
        enc.write(t3);

        let b = enc.bytes().unwrap();

        let exp = 25_usize;
        assert_eq!(
            exp,
            b.len(),
            "length mismatch: got {}, exp {}",
            b.len(),
            exp
        );

        let got = b[0] >> 4;
        assert_eq!(
            got, TIME_UNCOMPRESSED,
            "Wrong encoding used: expected packed_simple, got {}",
            got
        );

        let mut dec = TimeDecoder::new(b.as_slice()).unwrap();
        assert_eq!(
            dec.next(),
            true,
            "[t1]unexpected next value: got true, exp false"
        );
        assert_eq!(
            dec.read(),
            t1,
            "read value mismatch: got {}, exp {}",
            dec.read(),
            t1
        );

        assert_eq!(
            dec.next(),
            true,
            "[t2]unexpected next value: got true, exp false"
        );
        assert_eq!(
            dec.read(),
            t2,
            "read value mismatch: got {}, exp {}",
            dec.read(),
            t2
        );

        assert_eq!(
            dec.next(),
            true,
            "[t3]unexpected next value: got true, exp false"
        );
        assert_eq!(
            dec.read(),
            t3,
            "read value mismatch: got {}, exp {}",
            dec.read(),
            t3
        );

        assert_eq!(
            dec.next(),
            false,
            "unexpected next value: got true, exp false"
        );
    }

    #[test]
    fn test_time_encoder_rle() {
        let mut enc = TimeEncoder::new(512);

        let mut ts = Vec::with_capacity(500);
        for i in 0..500 {
            ts.push(i as i64);
        }

        for v in &ts {
            enc.write(*v);
        }

        let b = enc.bytes().unwrap();

        let exp = 12_usize;
        assert_eq!(
            exp,
            b.len(),
            "length mismatch: got {}, exp {}",
            b.len(),
            exp
        );

        let got = b[0] >> 4;
        assert_eq!(
            got, TIME_COMPRESSED_RLE,
            "Wrong encoding used: expected packed_simple, got {}",
            got
        );

        let mut dec = TimeDecoder::new(b.as_slice()).unwrap();
        for v in ts {
            assert_eq!(
                dec.next(),
                true,
                "unexpected next value: got true, exp false"
            );
            assert_eq!(
                dec.read(),
                v,
                "read value mismatch: got {}, exp {}",
                dec.read(),
                v
            );
        }

        assert_eq!(
            dec.next(),
            false,
            "unexpected next value: got true, exp false"
        );
    }

    #[test]
    fn test_time_encoder_reverse() {
        let mut enc = TimeEncoder::new(3);

        let ts = vec![3_i64, 2_i64, 0_i64];

        for v in &ts {
            enc.write(*v);
        }

        let b = enc.bytes().unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            got, TIME_UNCOMPRESSED,
            "Wrong encoding used: expected uncompressed, got {}",
            got
        );

        let mut dec = TimeDecoder::new(b.as_slice()).unwrap();
        for v in ts {
            assert_eq!(
                dec.next(),
                true,
                "unexpected next value: got true, exp false"
            );
            assert_eq!(
                dec.read(),
                v,
                "read value mismatch: got {}, exp {}",
                dec.read(),
                v
            );
        }

        assert_eq!(
            dec.next(),
            false,
            "unexpected next value: got true, exp false"
        );
    }

    #[test]
    fn test_time_encoder_220second_delta() {
        let mut enc = TimeEncoder::new(256);

        let mut ts = Vec::new();
        let now = time::now();
        for i in 0..220 {
            ts.push(now.add(Duration::from_secs(i as u64)).as_nanos() as i64);
        }

        for v in &ts {
            enc.write(*v);
        }

        let b = enc.bytes().unwrap();

        // Using RLE, should get 12 bytes
        let exp = 12_usize;
        assert_eq!(
            exp,
            b.len(),
            "length mismatch: got {}, exp {}",
            b.len(),
            exp
        );

        let got = b[0] >> 4;
        assert_eq!(
            got, TIME_COMPRESSED_RLE,
            "Wrong encoding used: expected uncompressed, got {}",
            got
        );

        let mut dec = TimeDecoder::new(b.as_slice()).unwrap();
        for v in ts {
            assert_eq!(
                dec.next(),
                true,
                "unexpected next value: got true, exp false"
            );
            assert_eq!(
                dec.read(),
                v,
                "read value mismatch: got {}, exp {}",
                dec.read(),
                v
            );
        }

        assert_eq!(
            dec.next(),
            false,
            "unexpected next value: got true, exp false"
        );
    }

    #[test]
    fn test_time_encoder_quick() {
        let data = vec![
            vec![
                4005208262633526538,
                4954753932961361627,
                -8498686817779589330,
                997288554975707767,
                420574790839486643,
                -6985361117436849476,
                6695217382881734993,
                -4882615480185378021,
                6979659359705572552,
                2951470135799522433,
                8634182340707554122,
                -8962139133632754294,
                3913352513302563351,
                -5372454418031781213,
                56877344526436144,
                -527537134533254567,
                -556819241767205213,
                -7387973212679104219,
                1561420719392635600,
                6017601653361707599,
                -2659672136519207877,
                1238241829611858534,
                2472201845568996436,
                5946741050211112280,
                -7670725594859425566,
                4614147708065781611,
                -5801829386695816107,
                -7056780630209177896,
                8988629168824467440,
            ],
            vec![
                -703161962856723496,
                -7684226850970599594,
                -8220489470326669076,
                2113768340419936616,
                3404325109769168471,
                7618036927502423716,
                8016890278453126046,
                7331396346252831605,
                404045216639148992,
                -7581302403165652888,
                1021316101599022682,
                44616686332962225,
                -1525985817335822631,
                -4321189194871944422,
                6853847775397353401,
                -39619373851837156,
                8044141247953953350,
                -3230612637944945887,
                -540377879243552600,
                6646041517845089700,
                -3577244019713562888,
                -8446955058550734599,
                -790711904365467021,
                8648644720060490432,
                1843148806078468022,
                -7814605898160193257,
                -4750007217294761397,
                -7606580286771803611,
                558854240110932083,
            ],
            vec![
                -5391864150131551774,
                5093501742335617294,
                -2198078342658035849,
                1337174778606812254,
                -7498679535335552710,
                -8018262214364301053,
                -100229050270415082,
                -5417050065665509200,
                4373033436267300607,
                1713122204119265218,
                -4803127442657754351,
                -1877324422972848793,
                -3826767789101641954,
                7166111808221056721,
                -1109967019025208756,
                -8186080074498300241,
                -3274506132719730960,
                256070925538650876,
                -6027732909851259158,
                -4706647663405279335,
                413315049122257810,
                -5214797624892062975,
                -6279844197682776085,
                5858982962135836681,
                2865455290634897527,
                4066755357646749639,
                -6926361836275593614,
                6200128162549190056,
                -3447657681632083557,
                -5312376467620706217,
                8960195261013132368,
                7074753761241024985,
                7340726686850598125,
                8331188209132206865,
                -1740922012923264661,
                8757874823281019449,
                4884871892553690232,
                -7517244963368965241,
            ],
            vec![
                690529877483568689,
                5985257362808491361,
                -4236039046048379180,
                1582219658615742667,
                8946000908914016317,
                2967044912691951112,
                3678084787751828887,
                4437898328840931549,
                7954448237111090575,
                -2607846619764247770,
                463884729549055998,
                -4970189800589774726,
                1564993318151862073,
                -7078365313186328526,
                -4272948425187258713,
                -3377104557098953226,
                -5133274710361516699,
                2929247308464114725,
                -2839085782017689134,
                -2955380283610442596,
                -7102987095551280689,
                1712841488286104887,
                5283499526703134100,
                800517390189351400,
                6943470780183411193,
                -762980247213399225,
                -930474992044383860,
                -372633136521085317,
                -3701470779221308610,
                -4480827467959962368,
                4239195729797166183,
                -4768263619380250726,
                8852928348180148292,
                -5892919145505126042,
                -3201459989038842709,
                5004063569027122293,
                2971866956600173683,
                2618043002132758095,
                -7499163143750607529,
                3005858281963055792,
                -6591758123998562637,
                -4006812602181544679,
                -4325464603040365769,
                -6504066551630476691,
            ],
            vec![
                -1233538211469356241,
                -3591152326522258411,
                -4551338452514471183,
                480613526312184947,
            ],
            vec![-335952565982510935, -3777526891403548296],
            vec![
                -2320711686543110539,
                3785078306266339807,
                -8442325088077980667,
                4426989202321261948,
                -8357730578287258465,
                7439617331197973439,
                3945309625104059576,
                6739669134720828700,
                -263256217421478122,
                3928756597094254066,
                5078585876406640442,
                2579609440550486073,
                757382702172250863,
                -199898189912865357,
                -3757290648149216747,
                -6351959599403806674,
                6322303149975204112,
                -1797613607839241127,
                5731947088214333409,
                -8265461390235306193,
                5302720606043798835,
                1908154346631553341,
                5900259192426133655,
                -6058190382408561581,
                5068348467263031846,
                5579293539583461557,
                -3543893625813124768,
                -22772531845017930,
                -5710480754967889475,
                -5475279482306285201,
                5904174662905855521,
                6513923058036358754,
                -4625868888988265988,
                7106298014004726606,
                -4024082768244512808,
                4232107638835037497,
                -7469006913389725388,
                -1056260484427539352,
                2005832656214014390,
            ],
            vec![
                4070041900932941413,
                4820033531936460694,
                -1153369643452134141,
                7641566509196685658,
                7324792580719196154,
                -1726760711793074985,
                3714825595140664748,
                7112615208636053113,
                -2184403971948262117,
                3722206433597628876,
            ],
            vec![
                3897775403169840063,
                -174039831665753987,
                -1435341535215823275,
                -5539129142638950940,
                4634341620319491904,
                8242548542851458466,
                7786568734475356482,
            ],
            vec![
                3073352221468687834,
                -2136469225848700425,
                -7963341368900164511,
            ],
            vec![
                -1208080938277557858,
                394054348011354858,
                4211066728223710319,
                543058918574870391,
                3859827073281426390,
                1144449214373602485,
                3733754000587734620,
                -7194572198131215221,
                -7981038302064877446,
                5936413027589499349,
                -267953016924040037,
                -731733558934293222,
                3783220641050505250,
                6422834829823406240,
                6821214689059039107,
                -5482252427837571465,
                -4572360449469797821,
                911858322704464438,
            ],
            vec![
                -2155780585298139505,
                -113312433279815382,
                -8074647709647751203,
                -2638513134234778035,
                4422750051133117200,
                7082402552469385711,
                2570760148060517308,
                1640617090410075757,
                5470278747513283614,
                -4301066156229960070,
                -4334996958129427896,
                -3812641084031168514,
                -5231158696330508344,
                -2008723129125113523,
                3470887800130985245,
                7446140864185308828,
                -86789779979766887,
                420245391535764887,
            ],
            vec![
                3210818789163542334,
                6896170657523508257,
                6241641089281784825,
                7931910482485133145,
                -386026756040895100,
            ],
            vec![
                -8222980124921059216,
                -220901287804231242,
                -7177953903044765047,
                -7180454761797937863,
                -3146433926462968560,
                -8382628296390093209,
                6354778791338118046,
                -3341897235735251706,
                -8121516330466594366,
                -1528704056765906840,
                -879925618888840377,
                654895892171161424,
                2970766131389904012,
                -3434328370651255587,
                1916133696942244298,
                825468454531836486,
                -1220602385896928192,
                -7638871101001517307,
                1794775228152511288,
                -2968859979789218161,
                -7520577328339129798,
                -455211347583345923,
                -6834703017329434738,
                641986413573198735,
                -4154094341692369579,
                5373301783955172101,
                2201074552938554549,
                9113307853800701862,
                -7350336019412145546,
                1845201337898320087,
                8841612052985284918,
                -2054488968323613801,
                -7244468147357636144,
                -3045729762091396767,
            ],
            vec![
                -5534709138746416748,
                5362292765331125242,
                -3607320457476159720,
                1059903744640628797,
                -7839359625920533114,
                -8758760098856450637,
                2263169929839931099,
                -1163727874402915540,
                6149024769527282044,
                4445384615840319498,
                -5715876324945983477,
                -889225243661938468,
                -1341874941461156677,
                2981890297011075351,
                3832045518679946446,
                7031741386479456150,
                8010352534175144779,
                2867425199837078927,
                -6920050499941110156,
                -7602380734530990901,
                5601231573598247986,
                -6972974512650734027,
                3798930373403556807,
            ],
            vec![
                3772832325811109120,
                5365084147281678453,
                -7091675647662796637,
                6207253951930597863,
                -4662945788698600491,
                730197908386329111,
                -443335888758860214,
                -8106710245700771763,
                2377064361394864455,
            ],
            vec![
                5174896157489456695,
                -5093226244810236258,
                3943069925580869398,
                -2265780137718676399,
                -8680758437535947895,
                -8028432077247912300,
                5781780587926295219,
                -6803945781782590546,
                -927494936574434573,
                -5832142163546361754,
                3530310803741994331,
                4974645891965671560,
                8272683526274722429,
                8190899915776767044,
                7157534373835683975,
                2079017264623585675,
                -8793187076848857973,
                -82690575754604724,
                -5279189609691699746,
                7990310540649682776,
                -8662576246609044889,
                6481475725577211191,
                1131771545975063515,
                -4042619375051770825,
                3951735150177187520,
                -6588480897669666252,
                5044831394324785904,
                -4236257154042154570,
                -7949675389989790710,
                507360066638229300,
                -7334332118040544995,
                2351515651743618735,
                -1393401756272641284,
                358836628260753929,
                -8064996029515210140,
            ],
            vec![
                -3863414524373128601,
                5536218594925541761,
                -755598213872050843,
                -2126471101286631394,
                -3800006900486355420,
                -2553274849459323037,
                -7375614347098590708,
                8776012137803200353,
                6976199345389673578,
                5256119802824054902,
                3922761453893886866,
                7188187457247686631,
                4742817147762062840,
                6108863763951590660,
                2892267774508715187,
                3703935540435415449,
            ],
            vec![7819189222679104203, -8401553165488411482],
            vec![
                -6465042601203582393,
                1212499522990052442,
                1782574706952265318,
                6969966407810835692,
                8774143266638255006,
                -243217066057183304,
                -1525790748920173746,
                1136965132143622848,
                -4919861316406222275,
                2702140629884537508,
                -2083773573440302013,
                1991527935926942909,
                3970871811130269111,
                7707421511112851248,
                -8999125810950683726,
                -6392907917973125874,
                -4051014071431965172,
                -5405782927897518010,
                -5854938396162801566,
                -8546035860726968843,
                -265511561064769435,
                -174781304007786011,
                -960713127308132648,
                -5722544321483209827,
                4821972809649378298,
                7602493137197371677,
                938901182031843047,
                -4360284719826982643,
                -723432411726891032,
                2405070122114404288,
                5068334872001179488,
                6698346030986026850,
                8241181818321586734,
                3763618369865280283,
                2066312525085565248,
                -7999040508028542374,
                7599741560468588039,
                -2506356369105218779,
                -1212167182460928330,
                9036910362173903283,
            ],
            vec![
                -7691590329509845510,
                -995593915576220670,
                6309232705713180032,
                2149710767964012786,
                2129454885286562651,
                5254882624188809492,
                5820450694649406485,
                -6294137310605471083,
                4244279901360625482,
                -2264650736186514994,
                -974162149046175595,
                -4722008727748574141,
                2756926477313143986,
                7414351501307627664,
                -2595752414258031149,
                -8880671168568499335,
                -7346008561151300394,
                -6682698839272135198,
                2611533214974568848,
                8510138911742813458,
                -3900436690439510814,
                -3851142146160099086,
                -400185502106235697,
                4830604519612175687,
                8032888117931094681,
                -6657592557549882315,
                6998724544024280521,
                -5308201651271770365,
                8578095746809227503,
                -1155351078384144119,
                1936880921640103852,
                7808785919161430177,
                -198974104933462123,
                5101123271158466260,
                -6918714800154049369,
                5230333241003275263,
                -2974678966947871708,
                -7115108996362636829,
                4973952858944651943,
                -1789557671720361896,
                7024618302768754790,
                4311780930081051794,
            ],
            vec![
                -89141451963604234,
                -2648055227609441229,
                -8024723011326996849,
                7897297341246487274,
                -6598780041378132350,
                2720138922107060034,
                -3068825148639453126,
                -5627765837105652301,
                5079260363933032666,
                -8647072651806947997,
                -1662109322927972756,
                891802611053636302,
                -3264585961168055887,
                -7253080359342603517,
                -6733450758744405254,
                -6062169508302475448,
                7007932439921189645,
                5671083412685954628,
                4999074254714297674,
                -6685677175607510857,
                6857209709132554369,
                2473539049882567971,
                -5770135439160071272,
                -3606784599781958813,
                8232763925500465798,
                5597102813034079432,
                -8388510316902499147,
                1973496720253837745,
                -1785137913623290884,
                7531222789536585376,
                569500811458596906,
            ],
            vec![],
            vec![
                6451832103510552589,
                -210348719623946475,
                -857700674418192163,
                2041245616418832046,
                -7443709602241190587,
                -1671345921694078063,
                -581307070316007259,
                7876276219716097028,
                6787674830183539127,
                -6481476178160754002,
                -6218176536673910977,
                -8624674956010775240,
                8055059931501577149,
                5798816270997401305,
                8270986298510387530,
                -315501867149120489,
                6020612737146736330,
            ],
            vec![
                7392858160934750692,
                4149814451939492443,
                -7484962267438935311,
                -606393883365203813,
                -40652392910504640,
                7314689400819169303,
                3073771172254211407,
                -1049175459407454103,
                -2550728576545005530,
                -3707339957900748856,
                9044410410128688732,
                7368910293497615310,
                -5968994368269520152,
                -986250339840978455,
                -59950262644945748,
                -4259763489103318826,
                5429483014763714846,
                8055423320887132167,
                966366016255817213,
                -850558206364223825,
                8830008577109490389,
                7087137594968049459,
                -6909451255941519,
                -7578596260766655829,
                -886994220554919263,
                2992139095009159493,
                5149866549092474627,
                847356918111554247,
                -8001890308414142227,
                -7078683748570974358,
                4175249892890537647,
                7772097388534163768,
                5547523278878337502,
                2330898050470731978,
                -6816146646504808355,
                -473779090781713885,
                -244881404172241316,
                5258330190076779474,
                1368981459448794039,
                5336201380601326668,
                8368288705172373217,
                -1657292745202575447,
                -6587607203877776082,
                7641624156333951693,
                -2816482018632049135,
                3681004047328307679,
                -6194458156990641279,
                -5899388163782001443,
            ],
            vec![
                8665508249728782980,
                1825710741958936657,
                7690736753959704749,
                -8137213064505424837,
                1471696602000952340,
                5859881900866593842,
                -1316315783891526599,
                6546178294863408362,
                -978756520969282702,
                1022616408347675992,
                -3292501743087915412,
                1341492357149783055,
                7373346798252247982,
                2442450024421344323,
                659155921836125708,
                -2119606101973981608,
                3355572548574205541,
                -6797913291148376708,
                2835220568290837180,
                2345360707138087196,
                4215871791162059559,
                -481225465724156535,
                -5067514724231180457,
                -7108499935513147833,
                1991363132065814224,
                2226356618533202029,
                3450222893305843056,
                -3724317115069540249,
                9036328384363774065,
                412079542176846664,
                6776087066651640326,
                -6556805471423812678,
                -8800794755234826630,
                -7544529876907473390,
                -8932623883086671171,
                68891249759983402,
                -4547301320141952846,
                -6467849290837402298,
                -1226586734821248155,
            ],
            vec![
                -1849191848821228919,
                9020562319355527713,
                -4834853608570653987,
                3950885594581480974,
                -2010715225471177916,
                -3478843164553094194,
                3308284435591389023,
                -4309550340168295089,
                5030487400423108108,
                -159004736141303851,
                -9044829709387359018,
                -1365837096389136395,
                2617856122617901563,
                -8605698902082608745,
                5953938591043956624,
            ],
            vec![
                7016151485978676572,
                5513155869569823120,
                -570879244307958653,
                2385159257805578027,
                -2998015324430296036,
                -5477473544420794274,
                6454101116849341580,
                3441686360809581055,
                6965663359809400346,
                -7195231142968457391,
                -6697944001948423967,
                923633139309257300,
                -8865589670690535169,
                -4566576487229852097,
                -2148008256045057833,
            ],
            vec![
                -12702760866694778,
                -395681790717186949,
                5064762078043100679,
                641275931354535338,
                -2339335589653519658,
                7380244562028822570,
                -2325182600341597986,
                4944077555492909548,
                7232804143580648856,
                3216831330426240209,
                1787674569079646238,
                -8700628523803483028,
                3664316166583578293,
                -250612249969156767,
                4588576584734158316,
                -8941931044731964082,
                5846820229284355770,
                8404457452792406335,
                -7234455535560682157,
                991862802586798297,
                -2828132945838225492,
                -7829452000992828588,
                -8057822964217376561,
                -6275797966214935143,
            ],
            vec![
                -4361007076295290192,
                -9058508931309464326,
                -7475637723571853787,
                7085029678262560203,
                1772793468472233262,
                9044596289719663502,
                1948938727423315180,
                4035573941372124689,
                -4151243121702380254,
                7623911080146524406,
                -1481904959895500959,
                -6832913656607703625,
                -5068639003722523786,
                -2052105726980634804,
                -4787480916409774602,
                1443567486737846894,
                3712005790624169542,
                -583586937300928564,
                -2335604950687887913,
                -4104213794288331767,
                -6751165321578150190,
                -2301577541290875047,
                5481649921123437298,
                1483548406093963875,
                1961337620638810649,
                -8824142741708434652,
                4931767815720462775,
                -3932285150381792735,
                673379724192994425,
                3693771793443971277,
                -4659824864093011887,
                3724533894553197066,
                4742736638837282890,
                -616836568268820525,
                6345132771274377455,
                -3806323158864164719,
                408788755040748125,
            ],
            vec![
                -8306989556967051892,
                -1696794268719558756,
                9112892392250635215,
                4934809778844194171,
                3720606626621976367,
                -5210201195094554775,
                -3139789069132831474,
                -6743816687482391313,
                -8065347659709972840,
                1082332373493797738,
                -4442772839727567786,
                608439675959790181,
                9156198614673168648,
                3075417944523624846,
                3539949111153038732,
                -2335443372287648741,
                7517236614363124004,
                -4673133927293494410,
                1453499962513222461,
                2899613407569577982,
                -3728345249085767590,
                4159638762006591387,
                -8263581934882212961,
                1475108651257946814,
                557490409901960768,
                7220380448343237952,
                8609303309350781577,
                6744496675249949070,
                4338143023945067035,
                -7849189792198433229,
                4857700104482189946,
                -6676149746169180318,
                6850807609386952419,
                9213148588741818126,
                -6557821291100612622,
            ],
            vec![
                -1581138215424074248,
                -7314597125588012583,
                4914922117063518059,
                323811728990732454,
                -5406680009194238754,
                398134197495879745,
                -3592921759415076558,
                -3871619201891502603,
            ],
            vec![
                8493513633563533546,
                -1424806187697444025,
                -6785957862216998317,
                -2895647534397328997,
                -2699349715468622109,
                -7815924413682527070,
            ],
            vec![
                2526887856568299877,
                -6398122074284986383,
                3230405548517641349,
                -7669083747018451340,
                2678362568171187135,
                -2900598869341171050,
                -1585115378945426298,
                6900528252203829919,
                4318878056262221879,
                3832832731416014626,
                -6268670523105026242,
                -6109972125954643484,
                -8651766534323977269,
                5902253317348091811,
                3293447962783349558,
                -8335911843622480167,
                1326883513630847769,
                1732260066946096148,
                -6225376343155194283,
                99872504106745915,
                6111954430351257637,
                -2655314282257433290,
                7773192704633165455,
                2737573081233054836,
                3631374684349720772,
                1326847276315546082,
                -4670208661567320933,
                729484447774977546,
                -1413127710897722156,
                -638882997090449420,
                6278175491885651608,
                8032287399656559219,
                88335516029511830,
                2795706816204102329,
                5588672795756735032,
                -5912208278526456532,
            ],
            vec![
                -3947587898770074318,
                125731683694979635,
                -3252854628603897814,
                -8075400796754929201,
                -1096912877338043992,
                -9126222016676367923,
                5183673256735352994,
                3020866441525487310,
                2269304955391455164,
                -4860709601352882997,
                -1760767154384177226,
                -3753041199492740152,
                -7695755080062595137,
                2728575255299686293,
                7052664945991107365,
                -6444167461051580943,
                9151689448361343722,
            ],
            vec![
                4864141419425657276,
                -1024598038029977812,
                7394667041893667114,
                1548008452329098519,
                4627432169063550385,
                663952282084802637,
            ],
            vec![
                -5128808239015236398,
                8223218770561162461,
                -1289112307737146178,
                2481512396395244878,
                6297717516652707978,
                2967168954612979789,
                8166151736545518626,
                -8688154066607503795,
                2873956969045602369,
                -2956319573223381965,
                670670123591888600,
                3488872596415493304,
                -1936242008054435747,
                -6612056842616083407,
                -7174129665518725599,
                8100335688248333180,
                -2389026304136868669,
                -2804386562827270832,
                -4382285087552193683,
                4004732670426989972,
                -1801248380509263786,
                7679925467249112516,
                -4987529841077537005,
                -5489045461486443903,
                6509727664430643475,
            ],
            vec![],
            vec![
                7671017874969348744,
                5894446653134069540,
                -5504459620373572063,
                1254444364168990709,
                8186795385273484119,
                4491522226544000912,
                -3278449747882284078,
                -7627498701217343308,
                -2471148726881385459,
                2142188101921739638,
                433726895296230800,
                4130043721967110953,
                5326534762211327220,
            ],
            vec![
                -6377667899612611821,
                1686935967247496098,
                5556179496301629530,
                -7334701625023983800,
                5760415623557628290,
                2864395758253952688,
                -7494526691554042933,
                7390724080268884153,
                -5996623506387857187,
                3229321605501611814,
                7544473055595246320,
                1855873622565836202,
                -2734354384286121896,
                -1606541229738452883,
                -202026680085694632,
                6304052082739473280,
                2780697866335483860,
                7985416133856973605,
                -811034369128856975,
                -3892405427970791422,
                -3497083159447498286,
                -4660915105472503742,
                -6743621526207485561,
                -1023723394289392433,
                7799959933458599521,
                5973019364137380322,
                5745660822733992262,
                -4979575205260396077,
                7447323580817327624,
                2853073088008043100,
                7302490716211821789,
                5672473813996306843,
                -4313099077833673430,
                -3671527925432186917,
                835237456549418154,
                -6715580097290948191,
                2490709152214797325,
                7799139928942276711,
                -9030179716516408396,
                4961662517655062771,
                -1731312277787813024,
                -3616609006399355420,
                -4691913152883127284,
                -8532724463176712784,
                -115180779089669032,
            ],
            vec![
                4501590105450209852,
                -873696266111132768,
                2492752831884643588,
                -4434774072108492868,
                -240094735093658240,
                -2716564901163253125,
                -121191871389532599,
                -7863884580991041310,
                8581431145848893229,
                -1354577702327851455,
                2586208506293888402,
                8896807596723047125,
                7673017317833991488,
                5671016944521625121,
                -8199074311843650119,
                9201510973426843474,
                -2658303179038081353,
                -5612708356530654774,
                -3756143182224493818,
                5586604942483203045,
                -8883658720398565998,
                7437148492394687413,
                5114471897312521686,
            ],
            vec![
                -2734677092801366219,
                1183874440585755279,
                9134655361178735446,
                -7677791424319542371,
                -7114847443113973466,
                -451068584443361037,
                1772899083240249582,
                -2882926806727919866,
                7077719862684380215,
                -2526545229047311365,
                -6626787214013966591,
                707476704864139395,
                2942674272889785529,
                -5579933807520696194,
                -5031152313310357284,
                8117101353145228929,
                -3883203436123046574,
                7565884745114119020,
                823402770263291306,
                -2550149467288304781,
                6708994955881266085,
                324620717227980350,
                -2989621739098938111,
                7601451269880058839,
                -4870171492164379326,
                -3835622122953852852,
                -6744427742435699298,
                9200580889799578163,
                4145870774889323278,
                -2972442248313268252,
                7652959377079630601,
                1650157608886312473,
            ],
            vec![
                6528379800418749779,
                -2274919533403343921,
                1672925942011326254,
                5439127882358692075,
                7180126805287213886,
                3024895375936613160,
                2300370037171329993,
                -4300988235832257958,
                -4340462134962013074,
                993483242998899065,
                -4480251092641949126,
                -6565392707509632400,
                -4561921372736215562,
                -1717682346245883269,
                7087322808081226680,
                7586479667283176687,
                -9072408615818149464,
                -557839543655716048,
                -6535734949475683867,
                8976726429446031269,
                -1287439741200667909,
                4048398388114855157,
                -2000999539720328473,
                -1956483895758382180,
                3974600148002512925,
                -738837299292152337,
                -7686313821963319936,
                -1471356236357278765,
                -5326135220964786636,
            ],
            vec![
                -8800485973491231134,
                8729736137353176446,
                -2890559220341229092,
                8423466370324058153,
                -6772987399501718628,
                -3971562315142950894,
                2633848013086417641,
                -7126616802738590113,
                -3444752665148275461,
                3259631039786498276,
                5136606874902935939,
                -7845912828431827130,
                -7731375587329333435,
                -6401179538651633333,
                1598428470560434661,
                -1535207813490935928,
                7719580256108406126,
                1445024681489434409,
                3358210192579016560,
                3342886921881970151,
                -2834708983825417969,
                -1755919519431860259,
                -8275310252896145231,
            ],
            vec![],
            vec![
                2195813128003132923,
                1337035379527722792,
                -7953206479242059201,
                -4595051613618393635,
                -4670567628706593517,
                -4884895568144838582,
                5273539843288582825,
                1455223956104588417,
                867970835467951286,
                -633037190383056861,
                2631779151998482172,
                827274727438169469,
                20531487463081098,
                -7610289256745914713,
                -6179428163018006984,
                6560201704515250798,
                -5013227761338658375,
                -3896192784917391720,
                4724117326767753734,
                9050239020167842705,
                6448332941548784530,
                9087715355604824118,
                -448219828455619757,
                6042634637149437049,
                -4407486632963351787,
                3637602581161576817,
            ],
            vec![
                5388823502338497308,
                4083809990943991755,
                9146611568253723812,
                -8821128757413225432,
                -8780798664461753420,
                -1234054130800042757,
                9108730952179983128,
                -8062018154101817313,
            ],
            vec![
                -269062006089790042,
                -2524294869844553004,
                -5070394163673556630,
                5560088967387153233,
                6619760407033372525,
                -7296475554640331735,
                -5169844037799619487,
                2724085748384788854,
                8666431262838739333,
                1640355737819958210,
                7525553717396838884,
                1896577640634973645,
                231771031528872688,
                1153535372388391942,
                -6792983116459069734,
                1977478612399377449,
                7396664310006750707,
                -8212021873700680821,
                -4152269157731715980,
                3394115585014991098,
                -8401259329415309516,
                6581085885472506958,
                -8973846737902052871,
                -1849908606347781460,
            ],
            vec![
                2726265982319959630,
                -7820871477163378049,
                453884904612975015,
                -3724028538944520909,
                6082864299721890596,
                -901557599017407598,
                -8503342290350861674,
                8775073383872625807,
                5732557718965694358,
                2439811063382734821,
                1362846207895494498,
                -6589128077257800880,
                166123755176031965,
                7255766326391839770,
                -3215592863227199510,
                4381971104948808450,
                7480586651109542552,
                -387959658954802879,
                -8622365409964017458,
                -7807053287281465019,
                3988454567941976549,
                -732827630736356444,
                5276444352277083833,
                6267772936625968032,
                -4899637720487005843,
                37254712199401998,
                -8397194756239374291,
                357093124017859777,
                8669836499706930086,
                -456868047277907164,
                -5272472257833248368,
                2069547015958358906,
                3794383950803043497,
                -2658949054927455949,
                -8725572990041607054,
                6015504630390824024,
                -7160391383180521966,
                5928420594114636216,
                -8858069545314333313,
                6403763874495492673,
                1189541696808594742,
                -323729883539841437,
                4317609497325204313,
                -9137239778038983758,
                -4461132417438666192,
                -5684235548655163174,
            ],
            vec![
                -1606885964070606978,
                2585465298569215340,
                -8911697810853061286,
                4583541347235723243,
                -3354918545471824358,
                4720140878846123407,
                3228130375683934014,
                937021501206557482,
                4760707139501707505,
                4507004036894175427,
                8561313872140562286,
                2053075665656879434,
                -6537937905326910196,
                -125616398056427623,
                1421729711999364001,
                -6982873846228835642,
                3169371342856666066,
                3284709566491927494,
                7285356637329551048,
                5321068936601010271,
                -640175213482635670,
                -1052584886449918457,
                -3312683413860587947,
                -8578908612406018297,
                7978506682601032086,
                -8389630629895512804,
                -4779399648525710685,
                -504944064438338793,
                488951443619970885,
                -5896264608095931314,
            ],
            vec![],
            vec![
                -8298474816681633647,
                -4928985862441395586,
                567948073617941425,
                -8668522442421492014,
                -3963073558496765255,
                6828711625430107904,
                5427592409824065527,
                945231112243106044,
                -2302546465983392878,
                6171438837150744822,
                1652708607881425026,
                833416407012221878,
                1211532725064500629,
                -1388555560182456703,
                2145180499023013636,
                1949691769540494655,
                8635676910319554917,
                7855442757044422889,
                -9134505035640203708,
                8550327470639113948,
                4672491401332118517,
                -6254417214299637015,
                -9010909330616557605,
                3964449433885595928,
                1425107828758100974,
                -1556410518985184839,
                3829613393700155237,
                -1099948392358815301,
                -6841268172643365318,
                5288929402760977817,
                -13085345178068400,
                -8487188769163415232,
                -7107486452565160168,
                8332173239670117196,
                3551410842547643513,
                -4843501763269444578,
                2403597682278466766,
                176917389252755827,
                -3267347649392567578,
                459728608937565741,
                5105379476759046510,
                4495570629079805161,
                328648556881539580,
                7105835029378335910,
                -276742444395436292,
                715545800504476837,
                -2540442674483784970,
                -275760328167042105,
            ],
            vec![
                -338760376462333140,
                -7007874055867173715,
                -7027895671994577048,
                8019287390074158360,
                -1854904020402290956,
                -2457653979287327963,
                7272655539553632179,
                -5760372585973262903,
                1716087538068537374,
                4207055043039125563,
                -6880344162131586203,
                -1309080401208794974,
                -8718057044693700561,
                -6929973734013951622,
                -1187312455207886610,
                -6028992631841398975,
                -8429464682236896508,
                -3984929622940314739,
                2040865661324169997,
                7442446533738754988,
                4531742872157813272,
                -5201459219342989247,
                -4126571619817797735,
            ],
            vec![
                -8346934707776271714,
                -2397489944105409950,
                -3777633210651244443,
                7163313478456135593,
                -1677710220854187607,
                8913539058224327806,
            ],
            vec![
                -3804547602409468020,
                -6553515074501517961,
                -7824930907612756675,
                -8456185337963538110,
                -4894861105797127398,
                7447811835928869691,
                -3416828995652755774,
                -7872106427691636104,
                -9046827579871801223,
                -8713400963554690233,
                4908735016774616135,
                -1730385107112578047,
                6691641017364039731,
                -5795502328383274250,
                -8431132684463614126,
                -2897175133309235706,
                844509612553138703,
                6651543829104433283,
                3656637814292241639,
                -7165541542014047603,
                1215831790705911736,
                -168809326678735942,
                -4726028375800970103,
                2925478867291425956,
                -7936926071932476703,
                3117220348376879551,
                4178677808012800898,
                8587548051876660572,
                -396028520698824799,
                1495280601792494289,
                -1898696417754430795,
            ],
            vec![
                3844304673215004400,
                6417358794349224161,
                -3641949953128517502,
                4850512116342780740,
                -7805759741443565653,
                -5798877891522966050,
                -2339030848668917848,
                -760883150169266927,
                -5958582118857100173,
                2643416751068030169,
                3547367197983749841,
                2500366127881188761,
                3568337134406025872,
                9068935761635958815,
                1876305232683515601,
                -3490026734725733474,
                -7903347987547941755,
                -3808025246412495063,
                5383952904403631783,
                635945282318155126,
                8857790329264280294,
                1612226190702204359,
                -998569571160165121,
                6389323800179160648,
                7100527138513007791,
                -6972145169997468564,
                596754352978598716,
                6757088249382546700,
                3416561576743399066,
                -7956171906240483366,
                4298730625337405914,
                -1300505998383310563,
            ],
            vec![
                3714303280907052375,
                6176629952870625016,
                -2454894268464809918,
                -7143805730357017928,
                -5630064329607478900,
                2448939986356985735,
                917794167903589015,
                -8987716298500960786,
                -1111081423593184986,
                780725798637772226,
                -2145014097175178513,
                -3291217044309479352,
                527323145880842384,
                7358209207783553184,
                -2890915381357380812,
                2249034433205665834,
                -2660006696538399558,
                1081263908543750972,
                -7738796383919427694,
                5223172569520230410,
                -625614454840252576,
                -181814506856399840,
                -4986600209578192467,
                -3659082969580644590,
                -2604505378377438783,
                4992941744518571000,
                8703215982742500576,
                7013588929900375694,
                -8927370259106601109,
                7421923269983452291,
                -1010205275793028100,
                -5539496186497424107,
                -8305204159612865469,
                2197846147721918978,
                -3700809907969474780,
                -7940863123723796394,
                6976267706778821758,
                5491678396333458977,
                3804571165052048653,
                6835178716881567395,
                720514563837570389,
            ],
            vec![
                5060879431010102146,
                -3163705706085621524,
                7476287321533593566,
                4999947670048432710,
            ],
            vec![
                3065013510715736599,
                -6077284698111448927,
                -2680414026874279124,
                -7422737279667498304,
                -3139036854802101543,
                -7595647740836023512,
                -992773451099107209,
                7591956034546125948,
                8518595595661245917,
                1989115521655627490,
                2836910184568365361,
                -259538652085887288,
                -6453078205787032572,
                5085149163421332166,
                2693011611744331842,
                857644252106853387,
                14812685290441839,
                -3287112642069642890,
                -8287794854428821929,
                -3288558528295973929,
                8239263284800107860,
                -906525035077635100,
                8307332624204198428,
                3100005100679658182,
                -992226170827942409,
                157800810671324548,
                4895339818898568504,
            ],
            vec![
                -5103746793456024733,
                -7978818719604620390,
                -8450024060371773761,
                1275392019668188984,
                6948895472721108965,
                2169419520065104821,
                -8540405692555250482,
                -5314263337184889239,
                -952380285408441599,
                8753077711623878860,
                -3529016698830801194,
                7255267870430182298,
                4918465866827897370,
                7133248322270470219,
                -223586014089127414,
                -962612880630290857,
                -7850959084498695695,
                -1104952378382424539,
                -4372796901409154362,
                -2888528732451060845,
                1994721180725330465,
                -7519094242597063510,
                3016210435031550151,
                5587458364086416012,
                4709273030930702878,
                -6607773174075722172,
                -3355259917908805794,
            ],
            vec![
                -8189200515469810441,
                -8502363677472491117,
                7527870478419662737,
                -7616125246177534166,
                -2807633935696003563,
                -8298705059711087422,
                -3878657597549167775,
                -6274025368063231136,
                -4254298590929149349,
                -8224707784861778185,
                -3159457894559653449,
                -2009697988018656846,
                -8399466899020296642,
                4223966205943021606,
                -2077182924479896269,
                4081983628033079167,
                -7030900160076919778,
                -5840805222044969145,
                -7725678347810710167,
                2120174865394726534,
                -5633752303453575191,
                8712170214181133346,
                -7185376915833077492,
                -6544101306308289782,
                7018993701540333141,
                -5331305862068653858,
                3237165874104123040,
                -8120205992989562370,
                -5794565473375889645,
                -6919393131720147220,
                7499381749021996504,
                8916465006239558831,
                -991010895811329703,
                -8914603308599476285,
                -1002690828367980777,
                -8693699777579793410,
                -1555609897124646791,
                -1592853837762770203,
                -5770840647734650386,
                6149913169450003608,
                148443389031230869,
            ],
            vec![
                2867912915966885884,
                3823193576582904005,
                -3386688938639794193,
                6384889710011239792,
                -6638328169987683469,
            ],
            vec![
                -8587847986689886148,
                -4079351791615471089,
                -301966154816194130,
                -6430335294735912260,
                3391573834185185611,
                8327026153183902220,
                -2338569298456365491,
            ],
            vec![
                -6345000667241675594,
                -6673759557268929368,
                -5984527112072323467,
                8877945604758970733,
                -1574718994675005924,
                -3974070217334145055,
                1863853356500251925,
                2288831846909061952,
                -6874973016086418059,
                -7580948973392876816,
                7359966387333757849,
                -7165878134672503353,
                -3950184964062331553,
                -6273365950783293479,
                3944867406006088651,
                5004030381111461350,
                1580113294398794097,
                8446677807280734013,
                -1945301606200786023,
                -7209900735952868347,
                -723843589255103768,
                -7646758817299909396,
                6130512113454682649,
                -3268688348416184938,
                3380250111582840757,
                9189288328288790427,
                -2944827567403966368,
                -2536566483168359998,
                -7904067556420978636,
                6490879532249804547,
            ],
            vec![
                -1184312911279959360,
                -6166887660468282687,
                1418330948975928649,
                -5576391594228853408,
                3317561093271507050,
                7713339475320476840,
                3129347455905081558,
            ],
            vec![
                -4366221762793571093,
                7604592356600805690,
                3085591139931766743,
                -547794778833995003,
                7019074413087733270,
                1184029307003818328,
                5432001200668322569,
                -5294319227253543027,
                4221625905341200522,
                4641084752699198875,
                -3919728381499137052,
                -3706734415128336101,
                -3323340802162546823,
                9116862469634880738,
                -2402108273912483810,
                3013037763820376823,
                -3656923730746414014,
                7858072744854662387,
                -9168019451261333822,
                1869458529163820073,
                -4956577911086042252,
                -3084962809440429502,
                -9168894550051623483,
                3101305268339772875,
                -758115366629622638,
                2773841811212389415,
                -7354564699290511416,
                -2593689271178274859,
                998334635254160286,
                5668494175164849239,
                -2540311391518582082,
                5417795615804467103,
                4837056977510015515,
                -8084116714023438247,
                -2822454174619955017,
                614121285448241230,
                1444000715211434679,
                -8675321581247135765,
                8738626840479910647,
                -6182510822527048082,
                -9013543860868404261,
                5144264840042471624,
                5617203756455253982,
                3534431987621304295,
                3435769909555862713,
                2323966887295469375,
                4014197118252019372,
                -5091401186658826655,
                603643424220867681,
            ],
            vec![
                7457009641542094159,
                -7686225969496292518,
                -335836238139296733,
                -8380493200714243835,
            ],
            vec![
                6518786294347654034,
                8585300188564781686,
                -3165406017497753124,
                7553774347062671750,
                3793426023467563776,
                513781412239252457,
                -3199676840635697726,
                2113362586828580996,
                -7633936993207769094,
                3147301203729158157,
                3416881551865892396,
                -3693257558116913224,
                -5321987561111156091,
                -510076623567630238,
                -5161276023908255137,
                1141642043340055145,
                -6283245964229157112,
                -8708885566013453627,
                5879620806903903323,
                4525549827567777054,
                6014673318078382368,
                1875641717038367058,
                -6795356403426822992,
                -6150322487358118863,
                8336428651460123590,
                -4116452721152646665,
                -3644127204832626547,
                7741345428962707561,
            ],
            vec![
                8367205465920948913,
                7229321360716223639,
                -3849056587965195400,
                -6008411803594841483,
                -2111808436479967163,
                -4178355122558048721,
                6297432821320520991,
                -4661006236258728031,
                -875219987214631208,
                -3764196235916914769,
                4484335270228078073,
                -8311928276803572512,
                -1653170743213678605,
                8134008643166572513,
                -918355146988339788,
                7041752721840881699,
                -4724066191837804170,
                5812656762401271484,
                -3009688773485117769,
                2746386415101231879,
                3450360954840501397,
                1623642737293389541,
                5780631084473537396,
                6696787248948410686,
                5457477187508417254,
                1435796966933461159,
                2659968121553928092,
                -755107880550511677,
            ],
            vec![
                5402999635612483314,
                5299970903390631467,
                8440094819945976515,
                83810417175706007,
                8419089623589118706,
                179181215796348627,
                4641578337708175597,
                3244028150616572451,
                -6117010042676659069,
                2257391306296044831,
                -931500159471338175,
            ],
            vec![
                -6085701900139775512,
                7354500739665390872,
                6256044719255134469,
                -5663341079805700726,
                2707060466401287366,
                -4654608634759722874,
                3444339477760215907,
                3704881020036316647,
            ],
            vec![
                -3821299053788550400,
                5912423890859501635,
                2713543099207412940,
                -6752645004639061326,
                -6164615477727565145,
                3273089062445607908,
                5145721457942071312,
                -7184751313213365649,
                -5483363462141690022,
                8143713985090207605,
                -3691078507956568658,
                -6259023617425069316,
                -1440445999283562166,
                6200435447820761597,
                487853185935216717,
                -8869633463000480990,
                -2784460979718850770,
                -2589995217266123082,
                -4366214069427782006,
                -8024740485787638820,
                -1872233284692116467,
                -6611379144396648599,
                8672284250354258377,
                3794467614995093889,
                -9014398439510258912,
                1554045630890195538,
                -3798078704920591236,
                4730973000585629294,
                -151928296845659260,
                -5753406784051928164,
                6396174392944245111,
                -8259088435270622592,
                5425940951411236434,
                1619133370002364315,
                4293634361429584971,
                -1248172994631102635,
                -8926469175587382267,
                -5622516601962089865,
                3247981466560422839,
                6740993736474807954,
            ],
            vec![
                -4630294561676344723,
                4041404392097944913,
                -6080804375354790799,
                6105274400508025195,
                -5108248092457500019,
                -873388850995646779,
                -1035039360705027208,
                6665263400111495182,
                8298130407650682734,
                8166990049058202460,
                -3547759385743271506,
                6371574005617024218,
                -7770822880248803170,
                -6883486433504174358,
                4073166311703233750,
                9145719575671847376,
                -4279376084407840068,
                6697890459392729538,
                -8632202726710778091,
                -4591089096694184957,
                -4477959014149474647,
                4131937648641040569,
                2286298620570758499,
                1622014022938264579,
                5496577350953933836,
                -2803405810884269042,
                936485344801252770,
                6631034042585530262,
                4493072801676880300,
                -4569887413177341979,
                -4131751425047585821,
                -6527393104112246089,
                -1642948779443063387,
                -4679589388061587080,
                4016827285752552988,
                -953756183611927705,
                1134946008870701040,
            ],
            vec![
                -23863277872863324,
                -2012477944402553955,
                -6865326095596018571,
                -3701293631776291480,
                -8502446953456504511,
                -2510505665896225080,
                7590029387527060379,
                8883417546357709908,
                -3337197806597617961,
                2541731655474842023,
                -2473179835303093521,
                -234564295492190796,
                -150349758838614119,
                -8911577870229727585,
                238257705709733224,
                9007538140854373728,
                7555276361534973164,
                -2292661714469977563,
            ],
            vec![
                -3945455262526972469,
                101231226509761481,
                3190045595614814736,
                -2090218210033206125,
                9107298416165403791,
                -4848657590104457476,
                1653411381008928090,
                -3037048716127587573,
                6624560496001959430,
                6795835883803237379,
                1686340856054144227,
                -5527873307735997382,
                822028482006847653,
                -7291494532728930976,
                -2528455252062967552,
                5190504276697422495,
                7114238748862754717,
                8288739947603388844,
                8865469931728345145,
                -4678997329827807332,
                -2585476819683421693,
                -8262485622402786804,
                -4036527003115691715,
                2221076644003623923,
                -4337725277653836881,
                6788425095397853874,
                -1399149536942869928,
                8684344263850451634,
                -2455876123783995483,
                -8432831091264291312,
                1517062814585038320,
                7212155717137429710,
                7403231576471597730,
                -6113298773721354946,
                -5598939586850341262,
                -7250191493445563365,
                8907278920983932953,
                -8300471355007936662,
                -5305812188565009249,
                4074578568148187602,
                -1442331519257766523,
            ],
            vec![
                -9189443109227320900,
                3919007546943988621,
                663737946230505626,
            ],
            vec![
                3329209806026831964,
                7381692970208817224,
                3341357683066011890,
                -6369675411323928205,
                -4493420282941004833,
                4630599089139976896,
                965756986203782416,
                8868986057247367778,
                105953139018323133,
                1562341159973112118,
                4438145592578477797,
                -5213334249731598853,
                -6759057935154733992,
                8178776230089402493,
                -4971809643479545245,
                332351491674935565,
                -3098048154618497902,
                790660754494512901,
                -4006181184225387293,
                7574057873040494558,
                6325252653760032928,
                6248677611149595622,
                -190091588899808093,
                1175252024506916113,
                2401146525772281661,
                -2080582782581016341,
                -7923568981146038003,
                -9114116467119730940,
            ],
            vec![
                4283380854758380893,
                337378841052804938,
                2159546515715911516,
                7230979817978630881,
                8462866512312868542,
            ],
            vec![
                9094439836378814215,
                -1863609215824702091,
                -1410691915539323066,
                -6893024696014088961,
                1580865889153571757,
                -133687091991858588,
                -7159891415903900065,
                7594970301077462058,
                2319730962289405055,
                -2133761914650837558,
                576665479410507956,
                -8995306370705087492,
                -9053893282832997294,
                3136277138981356159,
                3204427449461430568,
                489793662506255059,
                155835506327885178,
                -7150335640438067863,
                -8852104065322709869,
                -7210871516503232852,
                5045524284397065729,
                -8401136059978686973,
                -2013209032989216739,
                -7039055005490799676,
                -5199633504047113557,
                1019391607760482446,
                -7470890768768157667,
                1841325917344979502,
                -2187786844421817845,
                3752263034316072669,
                -4131292814493584812,
                8358950507070926484,
                3184343433194580028,
                -1945310100708853352,
                -3930635510395761056,
                1119812212281254970,
                -9123290520742314867,
                8234558674629972850,
                4424315158659796649,
                -710398928411677633,
                5616890389002214718,
                1264607811941989588,
                6010960321850129514,
                -5778861671857470191,
                2571703137537674663,
                -1201811323747390487,
                -6746231721274431106,
            ],
            vec![
                4539735392624466120,
                -521717308265807082,
                -868108720693526612,
                -3445174445552816295,
                -1044029313893367150,
                -8490224611717884872,
                7409814137519504029,
                2197889724973502401,
                1464049605163200946,
                -6134057622530178205,
                7669192904888320784,
                6645724201142056900,
                -3228785590045309881,
                3913960344646840689,
                2585644651000714892,
                6590691088316236279,
                -8579024324519826058,
                -2018307215835127961,
                -7815600872893233480,
                -1108972492495154967,
                -341400704014548796,
                -903233881735456689,
                1899084971384661165,
                -6713689999407611850,
                1733915608749855813,
                8950366833596233155,
                7800337031271820228,
            ],
            vec![
                9152746178811664822,
                -8151349217436917309,
                -3310845518929384847,
                -3261769649839766158,
                -563997095795922612,
                -733057138273027286,
                2893336588614199957,
                7926290169792942119,
                1127625291831058158,
                -7084271471299940633,
                -4702919533865000096,
                -5090172215941409356,
                5811257823906382326,
                3303814819379886082,
                2102479614622823826,
                6860901054503614973,
                -4251290886204166719,
                3406154782868461765,
                -5121805052171860963,
                7413078342490665529,
                6427846380114465315,
                -576015051768436310,
                -8619509746192375063,
                -3870716667911410020,
                -484342871665407588,
                -4735811326856379914,
                -4565569294238101007,
                -844002313410753181,
                -4802159931549753644,
                -5478297818787297090,
                -3585460096458740679,
                2909218403511282245,
                1015176331816059101,
                -1511424255216927806,
                -5675376361076231582,
                -340960784060457248,
                -7026220765966181564,
                -6943476577243887848,
                8835714405098007455,
                -1388206574876418189,
                8046669795514005801,
                3612653108138694049,
                4445745424621100237,
            ],
            vec![
                -6402584083298030019,
                -8722829328605351322,
                8551287685896841316,
                1260182145848447530,
                -2531195866162355998,
                -837165165287170301,
                4415735675618208334,
                -3920362816495059792,
                -4011569798183280498,
                949673153090816343,
                5819464990152694569,
                4883364348023665357,
                -8222494900370307019,
                -6214071940901284685,
                1720861079523681797,
                2204884155205222672,
                4761918955935515302,
                -7442018639668519201,
                -7280590242191424275,
                -8271953798543121543,
                6602898470537946787,
                3964908309343405502,
                8823563610330981344,
                -7083600155343132283,
                2089705350946753533,
                -6228173412617550630,
                -1051163274050474086,
                7536884338658646903,
                3967609899369353746,
                3080392860604126541,
                -1709068463491578292,
                4239219511253405849,
                8687005981048357028,
                -5795292542142412730,
                -4850627748780072245,
                7515706853773044397,
                9217086812062372735,
                4979310232315090341,
                2479658275045117966,
                5511390036244375675,
                -6090518452953911932,
                -7351309509627569159,
                3342475514289556333,
                -5714407987232563456,
                -5476412448254977079,
                4175581199156612607,
                -3427388003432587907,
                -2635174950271005796,
                -4521788577390262532,
            ],
            vec![
                4985751845935566104,
                -3968800784585323693,
                -8624416423954741390,
            ],
            vec![
                -737085924734364971,
                83569377125090394,
                -1531874111711186659,
                1403891427697796623,
            ],
            vec![
                6637649102118618780,
                -810248815727962848,
                -6866214505428419155,
                -8649744547804527295,
                2445286031524803849,
                6898684026778279423,
                -373935772927741533,
                8572553322849533174,
                -7612701378788962471,
                7902585762471949189,
                3182479460125818802,
                -5181190446921038408,
                6727795718877828118,
                6982194568585515637,
                3117441661541311632,
                8777503959701230748,
                2619054820897496753,
                2499525242089681619,
                -8203917508013263270,
                3784628879354818112,
                7220494842112153602,
                -1877803181470531687,
                -4123037711486651500,
                -5929784390123389939,
                -8335507243432000945,
                -7476676577704239769,
                7276591943431452865,
                -8802480620126198688,
                936205685768273146,
                -1110469035999023468,
                5129282053043735395,
                6855771943709688299,
                5368279844861447642,
                1638489386259599913,
                8107075526826868543,
                45456624917387452,
                -4809823516073295855,
                8268213574010745590,
                -937253252412856293,
                721025328451856606,
                3050591960396876443,
                6256294090397137863,
                2828536066612182812,
                -5027038491723251462,
            ],
            vec![
                2071925714402943872,
                -2876132899617231906,
                4727598493869647984,
                5401987106888098606,
                1987885789263785733,
                -801089605985940158,
                3610652817674549582,
                3083567672336857899,
                8089809245852308808,
                802526245989192952,
                563601186902437306,
                2363397649819341830,
                -929115891532264577,
                -140321794829373819,
                -8967621098232716044,
                7530136359240048093,
                1210215986925127707,
                -3501762495357042321,
                -670226706358100828,
                1243910150500182703,
                7350646696215022208,
                2230657887259684309,
                -1233938316628885728,
                -7025506826716774030,
                358793800963006708,
                -5515300404547792983,
                5257350843818905364,
                -6876394009434963512,
                3355216509704356975,
                -2006217806781429926,
                -5582282226677951761,
            ],
            vec![
                7766803332215234955,
                1589530760968171091,
                -3243545815190547955,
                4673958616474589698,
                -4164161806233953148,
                3039105381007168038,
                4689978372681343613,
                -8955555798667434400,
                2942226119170636797,
                -1754150079785973091,
                3525397781895149596,
            ],
            vec![
                -5935987109730488541,
                6104413921722000167,
                -6211733874619459385,
                -8403764266997888977,
                -5001544897389009776,
                7149138230178028192,
                -9073509951777570438,
                -2134535504794426834,
                3906960560395553259,
                7711378002608254406,
                -3402375440408043526,
                -3779641304321059308,
                -1662485444343299131,
            ],
            vec![
                -4670328397819197556,
                -7327402077010470680,
                -2504426838455282945,
            ],
            vec![
                1951964227701396799,
                -2962460211006852119,
                1908124611482183708,
                -5420556087900968230,
                -3832774547166560813,
            ],
            vec![
                -4081524060598151294,
                5905739231122692084,
                5679386250937293550,
                5591383323611588951,
                -3953463865559032933,
            ],
            vec![
                8038466020848215716,
                -6286861461764339339,
                7273327118013302071,
                -7482106071675360872,
                7973888939657273191,
                509791938205614063,
                4439222125571789432,
                -522390981933756073,
                -5268650902871400175,
                5186226959239119682,
                -1282296061631228840,
                -6110252756921938044,
                4577634943883558836,
                -6164356170195092580,
                8568325694036540161,
                7486348918166034791,
                -3691970099144052664,
                2925997512409700012,
                3498987851800457942,
                5673008405877447538,
                -1959926690377013125,
            ],
            vec![
                5928277183377337420,
                1946046121446191261,
                476617326988353204,
                4800385454471322627,
                -5985485837071398377,
                4373766301911099953,
                -3653167634207869357,
                -674302576503934218,
                -624564449646452616,
                2078170890611442176,
                -4690056907318743526,
                -1006128676358814981,
                -3062393032087584069,
                9035447661137193653,
                -5099583516221563572,
                7729909491043186516,
                -6238849228938061355,
                -5044592336898521763,
                1417462693720734752,
                -5491633985597764105,
                2905224625756864405,
                -5432801890673375451,
                -299590457514589008,
                -4441731986305301956,
                743319642134482309,
                -5952310360746436705,
                8614530595456936422,
                -580156169442423607,
                -2046572214688232051,
                -2775211600550232760,
                433950971127864377,
                -6476251536427137971,
                4292004697222905592,
                3229428927671641164,
                -219275726033774829,
                -306340252021497769,
                -8407091317088028501,
                -6506311701376710936,
                -3936970267570782476,
                -1286037981999107700,
                -8783429774058322580,
                489270262795226235,
                2793738195812255844,
                -3058796409285431311,
                8002543097843670118,
                7912207223276227785,
                -5302730783144991862,
                757815754732042360,
            ],
            vec![
                -6004224375516703933,
                1299626665269930748,
                3031877924090353561,
                8425704918440022733,
                7852085779794716002,
                -8852264048009026579,
                -8209696499070727554,
                6897343115551954380,
                1174081452938288050,
                2888907647133651823,
                3643861264257482960,
                -939838184453913155,
                -4993235471778782382,
                -3623232691696894969,
                8593012590075271313,
                5956472037260063743,
                6759517071484642021,
                -7818696564458519056,
                5071518169181092686,
                3096128695351659884,
                -265258481289566711,
                1959482364925015444,
                -5866990005921878866,
                -3023392485678788840,
                639168638542851680,
                4999848553266396124,
                -4082799625411438816,
                -1749506627636670239,
                215190548938875881,
                2613195789553331096,
                6039378412911597875,
                1860268267978477604,
                2685217450765130188,
            ],
            vec![
                7255261957086155728,
                7543332469577593449,
                -8977602780920818696,
                -7123573083219459041,
                7123860715136165111,
                7504855058595854139,
                -5244730297828059799,
                -7358208222492813194,
                1138499381397694431,
                -8092772582746772916,
                -7202267265463429232,
                7208936459959977953,
                2706553567654322275,
                -4826208849047087452,
                1158205191475415358,
                -6712774311488696831,
                -7136865174190249598,
                -3555358995385513841,
                -8223089455085063401,
                -2582948219201574185,
                997992002909709074,
                -415389123506317025,
                -6315398614900978681,
                -2090411522106096083,
                -5743203591249016394,
                3694407799984493997,
                -3317233370787677246,
                7692853331522503281,
                -4272986729593943360,
                -653225441483189470,
                -2817429390508304024,
                -2205588051685497847,
                -3842275798923809316,
                7248972624335298138,
                2667301536835230526,
                484502691200043949,
                8760332452851894393,
            ],
            vec![
                1678646870430499065,
                -9046723783123213407,
                -3883881617064914886,
                -6753261764304174463,
                7232332432488687144,
                5606642666037132525,
                -8761826410522003791,
                -1788663907567026073,
                -8143253256107835258,
                -8370568395657496616,
                -4030301388086020430,
                -2376749337391640132,
                743796815162232690,
                7782450863360541225,
                -3918771547218898001,
                2293357734679144589,
                1145277242726351259,
                6022238620016503578,
                313464887578910582,
                -2186760427377726037,
                -8925024941235238112,
                7476113821555403773,
                -6429363441552141043,
                -2198673456709554149,
                1359677966623425772,
                6365423128101734471,
                -2928246377533512914,
                535344735369129897,
                -2130769319586663104,
                -4162513115915318357,
                -2296175495999972687,
                -5120974831089372819,
                -6986051273928031706,
                -4996426562482298279,
                -6964823750828417147,
                -1670243843320505763,
                821453829946442965,
                -3003162081219896291,
                6096504067840081422,
                7307779691385297364,
                -8881291916247826638,
                -1754204380460438963,
                -7517805818296629567,
                -3932744653176256598,
                -2640886698148240155,
                -3641778087489953535,
                -8586746101793390639,
                9045831460942070804,
                3790254559861270943,
            ],
            vec![
                3007323949022615105,
                4672605983335141022,
                -6693872632358479917,
                25130255727913770,
                -4090553146921998692,
                2415439387553741497,
                6564998516079824172,
                -7446020312794573885,
                595847068822062163,
                -1125405997224147483,
                344937009820722346,
                -5480678994047527238,
                7627480103427102570,
                2129811488724930194,
                -9221988818986541768,
                156928541639071952,
                539737909029573982,
                350919705416787875,
                -7794441987128283148,
                -3841615426605665299,
                976676022020331061,
                5019630693343823013,
                8800245374452188086,
                5381655051000667587,
            ],
            vec![
                3785641805975024726,
                4877466860911928687,
                1996093591767493513,
                -1384288075467499847,
                2509659376763050001,
                597051506672739932,
                3492237736306580745,
                -5634116868355673088,
                -9169055723904941075,
                8088291047856854628,
                -5951673069308402395,
                -5481051000168190280,
                -2364294317132347957,
                5676585122182028830,
                -7749914896575976734,
                -8092203405295005431,
                6327369997483517788,
                -7201210522361540721,
                -8783078196700424148,
            ],
            vec![
                -7269214942613290173,
                -2585647188565360585,
                -932121597779125438,
                -3766096750833781380,
                -2195259361366918212,
                -745482833033066046,
                -5228091403124796732,
                2077006072983929459,
                461705664730456168,
                8766201846492734508,
                4563661410494606499,
                1110748499529194374,
                -6171062507910118549,
                6128037204399177182,
                3373652724954417525,
                3386049654989810348,
                -5178790841263929236,
                7021858819507152503,
                -3080972728898556796,
                5343584584626062748,
                -5810339262425432360,
                4707728878682146765,
                -4395699386917719803,
                -4161334546019958147,
                -8561399797857193381,
                -5342066881963379657,
                3486722833082964723,
                6313498171535054141,
            ],
            vec![
                -6945492449963692,
                4463407711864925561,
                7484603325294467279,
                -3598059785615731949,
                -34456318615044312,
                -5083690336650602934,
                -4777759199956059333,
                -2957651655737507375,
                -3791723985487432321,
                -4140748388609348982,
                1604213528333344986,
                -3983628344735390770,
                9069821214711766328,
                443366004270026343,
                3600335154375967603,
                5292180098257442815,
                280045098341883278,
                -1672107164343460247,
                -635699305848040017,
                5523329968359725044,
                -1699642149583608962,
                2514539274686489546,
                -3359618607469117099,
                -172050642208797890,
                4289648606294970843,
                1666827221652060245,
                5614131182663481433,
                2352862743697991237,
                -6269789279182618574,
                1755954532546383168,
                1651869351495269317,
                7201531980243604288,
                -8066068619652414718,
                -3549802679234938050,
                5679459972381951319,
                -8429770733645832885,
                3658816405111928277,
                2415549731664247114,
            ],
        ];

        for values in data {
            let mut enc = TimeEncoder::new(1024);
            let mut exp = Vec::with_capacity(values.len());
            for v in &values {
                exp.push(*v as i64);
                enc.write(*v as i64);
            }

            // Retrieve encoded bytes from encoder.
            let buf = enc.bytes().unwrap();

            let mut got = Vec::with_capacity(values.len());
            let mut dec = TimeDecoder::new(buf.as_slice()).unwrap();
            while dec.next() {
                assert_eq!(dec.err().is_none(), true);
                got.push(dec.read());
            }

            assert_eq!(got.len(), values.len());
            for i in 0..got.len() {
                assert_eq!(got[i], values[i]);
            }
        }
    }
}
