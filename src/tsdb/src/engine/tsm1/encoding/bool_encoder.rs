//! boolean encoding uses 1 bit per value.  Each compressed byte slice contains a 1 byte header
//! indicating the compression type, followed by a variable byte encoded length indicating
//! how many booleans are packed in the slice.  The remaining bytes contains 1 byte for every
//! 8 boolean values encoded.

use crate::engine::tsm1::encoding::varint_encoder::VarInt;
use anyhow::anyhow;

/// Note: an uncompressed boolean format is not yet implemented.
/// booleanCompressedBitPacked is a bit packed format using 1 bit per boolean
const booleanCompressedBitPacked: u8 = 1;

/// BooleanEncoder encodes a series of booleans to an in-memory buffer.
pub struct BooleanEncoder {
    /// The encoded bytes
    bytes: Vec<u8>,

    /// The current byte being encoded
    b: u8,

    /// The number of bools packed into b
    i: usize,

    /// The total number of bools written
    n: usize,
}

impl BooleanEncoder {
    /// NewBooleanEncoder returns a new instance of BooleanEncoder.
    pub fn new(sz: usize) -> Self {
        Self {
            bytes: Vec::with_capacity((sz + 7) / 8),
            b: 0,
            i: 0,
            n: 0,
        }
    }

    /// Write encodes b to the underlying buffer.
    pub fn write(&mut self, b: bool) {
        // If we have filled the current byte, flush it
        if self.i >= 8 {
            self.flush();
        }

        // Use 1 bit for each boolean value, shift the current byte
        // by 1 and set the least significant bit accordingly
        self.b = self.b << 1;
        if b {
            self.b |= 1;
        }

        // Increment the current boolean count
        self.i += 1;
        // Increment the total boolean count
        self.n += 1;
    }

    fn flush(&mut self) {
        // Pad remaining byte w/ 0s
        while self.i < 8 {
            self.b = self.b << 1;
            self.i += 1;
        }

        // If we have bits set, append them to the byte slice
        if self.i > 0 {
            self.bytes.push(self.b);
            self.b = 0;
            self.i = 0;
        }
    }

    pub fn bytes(&mut self) -> anyhow::Result<Vec<u8>> {
        // Ensure the current byte is flushed
        self.flush();
        let mut b = Vec::with_capacity(10 + 1);

        // Store the encoding type in the 4 high bits of the first byte
        b.push((booleanCompressedBitPacked as u8) << 4);

        // Encode the number of booleans written
        let mut tmp = [0u8; 10];
        let s = self.n.encode_var(&mut tmp);
        b.extend_from_slice(&tmp[..s]);

        // Append the packed booleans
        b.extend_from_slice(self.bytes.as_slice());

        Ok(b)
    }
}

/// BooleanDecoder decodes a series of booleans from an in-memory buffer.
pub struct BooleanDecoder<'a> {
    b: &'a [u8],
    i: isize,
    n: usize,
}

impl<'a> BooleanDecoder<'a> {
    /// initializes the decoder with a new set of bytes to read from.
    /// This must be called before calling any other methods.
    pub fn new(b: &'a [u8]) -> anyhow::Result<Self> {
        if b.len() == 0 {
            return Err(anyhow!("no data found"));
        }

        // First byte stores the encoding type, only have 1 bit-packet format
        // currently ignore for now.
        let b = &b[1..];
        let (count, n) = u64::decode_var(b).ok_or(anyhow!(""))?;
        if n <= 0 {
            return Err(anyhow!("BooleanDecoder: invalid count"));
        }

        Ok(Self {
            b: &b[n..],
            i: -1,
            n: count as usize,
        })
    }

    // Next returns whether there are any bits remaining in the decoder.
    // It returns false if there was an error decoding.
    // The error is available on the Error method.
    pub fn next(&mut self) -> bool {
        self.i += 1;
        self.i < self.n as isize
    }

    /// Read returns the next bit from the decoder.
    pub fn read(&mut self) -> bool {
        // Index into the byte slice
        let idx = self.i >> 3; // integer division by 8

        // Bit position
        let pos = 7 - (self.i & 0x7);

        // The mask to select the bit
        let mask = (1 << (pos as u8)) as u8;

        // The packed byte
        let v = self.b[idx as usize];

        // Returns true if the bit is set
        v & mask == mask
    }

    #[inline]
    pub fn err(&self) -> Option<&anyhow::Error> {
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::tsm1::encoding::bool_encoder::{BooleanDecoder, BooleanEncoder};

    #[test]
    fn test_time_encoder() {
        let mut enc = BooleanEncoder::new(0);
        let b = enc.bytes().unwrap();

        let mut dec = BooleanDecoder::new(b.as_slice()).unwrap();
        assert_eq!(
            dec.next(),
            false,
            "unexpected next value: got true, exp false"
        );
    }

    #[test]
    fn test_boolean_encoder_single() {
        let mut enc = BooleanEncoder::new(1);
        let v1 = true;
        enc.write(v1);

        let b = enc.bytes().unwrap();

        let mut dec = BooleanDecoder::new(b.as_slice()).unwrap();
        assert_eq!(
            dec.next(),
            true,
            "unexpected next value: got false, exp true"
        );
        assert_eq!(
            dec.read(),
            v1,
            "unexpected value: got {}, exp {}",
            dec.read(),
            v1
        );

        assert_eq!(
            dec.next(),
            false,
            "unexpected next value: got true, exp false"
        );
    }

    #[test]
    fn test_boolean_encoder_multi_compressed() {
        let mut enc = BooleanEncoder::new(10);

        let mut values = Vec::with_capacity(10);
        for i in 0..10 {
            let v = i % 2 == 0;
            values.push(v);
            enc.write(v);
        }

        let b = enc.bytes().unwrap();

        let exp = 4;
        assert_eq!(
            exp,
            b.len(),
            "unexpected length: got {}, exp {}",
            b.len(),
            exp
        );

        let mut dec = BooleanDecoder::new(b.as_slice()).unwrap();

        for (i, v) in values.into_iter().enumerate() {
            assert_eq!(
                dec.next(),
                true,
                "unexpected next value: got false, exp true"
            );
            assert_eq!(
                dec.read(),
                v,
                "unexpected value at pos {}: got {}, exp {}",
                i,
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
    fn test_boolean_encoder_quick() {
        let data = vec![
            vec![true, false, true, true, false, true, true, false],
            vec![
                false, false, true, true, true, false, false, false, false, true, true, true,
                false, false,
            ],
            vec![true, true, false, true, true],
            vec![
                true, true, true, false, false, false, true, true, true, true,
            ],
            vec![
                true, false, false, true, true, false, true, true, false, true, true, true, true,
                false, true, true, false, false, true, true, false, false, false, true, true,
                false, true, false, false, false,
            ],
            vec![
                true, true, true, false, true, false, true, true, true, false, false, true, true,
                true, true, false, false, true, false, false, true, false, false, false, false,
                true,
            ],
            vec![false, false, true, true, true, false],
            vec![
                true, false, true, true, false, true, false, false, true, false, true, true, false,
                false, true, false, true, false, false, true, true, true, true, false, true, true,
                true, false, false, false,
            ],
            vec![
                true, true, true, true, true, true, true, true, true, true, false, false, false,
                true, false, true, false, false, false, false, true, false, false, false, true,
                false, true,
            ],
            vec![
                false, false, true, true, false, true, false, true, false, false, false, false,
                false, false, true, true, false, false, true, false, false, true, true,
            ],
            vec![
                false, true, true, false, false, true, false, true, true, false, false, true,
                false, true, true, false, false, true, true, false, false, true, true, true, true,
                false, false, true, true, false, false, true, true, false, true, true, false,
                false, true, false, true,
            ],
            vec![true, true, true, false],
            vec![
                false, false, false, true, false, false, false, false, true, true, false, true,
                false, true, false, false, false, false, false, true, false, false, true, true,
                true, true, false, false, true,
            ],
            vec![
                true, true, false, false, true, false, false, false, false, false, false,
            ],
            vec![
                true, true, true, false, false, false, false, true, false, true, true, true, true,
                true, true, true, false, false, false, false, true, true, true, true, false,
            ],
            vec![true, false, true, false, true],
            vec![false, false, true, false, false, false, true, true, true],
            vec![
                true, true, true, true, false, false, false, false, true, true, false, false,
                false, true, true, true, true, true, false, true, true, true, false, false, false,
                false, false, false, true, false, false, false, true, true, true, true,
            ],
            vec![
                false, true, false, true, false, false, true, true, true, true, false, true, false,
                false, false, false, true, false, true, false, true, false, true, false, false,
                true, true, false, false, false, true, true, true, false, true, false, false, true,
                false, true, false, true,
            ],
            vec![
                false, true, false, true, false, true, true, true, true, false, true, false, false,
                false,
            ],
            vec![
                true, false, false, true, false, false, true, true, false, true, false, false,
                true, true, true,
            ],
            vec![
                false, false, false, true, true, false, true, false, false, true, false, false,
                true, true, true, false, false, false, true, true, false, false, false, false,
                false, false, true, true, true, true, true, true, true, false, true, true, false,
                true, false,
            ],
            vec![
                false, false, true, true, true, true, true, false, false, true, false, true, false,
            ],
            vec![
                true, true, true, true, false, true, true, true, false, true, false, false, false,
                false, false, true, true, false, true, false, false, false, true, true, true, true,
                false, false, false, true, false, false, false, true, true,
            ],
            vec![
                false, true, true, false, false, true, true, true, true, false, true, true, false,
                true, true, true, false, false, true, true, false, false, false, false, false,
                true, false, true, false, true,
            ],
            vec![true, true, false, true],
            vec![
                false, true, false, true, true, true, true, false, false, true, true, true, false,
                false, true, true, true,
            ],
            vec![
                true, false, false, false, false, false, false, false, false, true, true, false,
                true, false, true, true, false, false, true, false, false, true, true, true, true,
                true, true, false, false, true, false, true, true, false, true, true, true, true,
                true, false, false, true, true, false,
            ],
            vec![false, false, true, true, true, true, true, true, false],
            vec![true, true],
            vec![
                false, false, false, true, true, true, true, true, false, true, false, false,
                false, true, false, true,
            ],
            vec![
                false, false, true, true, false, true, false, false, false, false, true, false,
                true, false, false, true, false, true, true, false, true, true, false, true, true,
                true, false, true, false, true, true, true, true, true, true, false, false, true,
                false, true, false, true,
            ],
            vec![false, false, false, false, false, true],
            vec![
                false, false, false, false, false, true, true, false, true, false, true, true,
                false, true, true, false, true, false, true, true, true, false, false, true, false,
                true, true, false, true, false, false,
            ],
            vec![true, true, true, true],
            vec![
                false, true, true, true, false, true, true, false, false, true, true, true, true,
            ],
            vec![false],
            vec![
                true, true, false, true, false, false, true, false, false, true, false, true,
                false, true,
            ],
            vec![
                false, true, false, false, true, false, true, false, false, false, true, false,
            ],
            vec![
                false, false, false, true, true, false, true, true, true, false, false, false,
                false, false, true, false, false, false, false, false, true, false, false,
            ],
            vec![false, true, true, false, true, false],
            vec![
                false, true, false, true, true, false, false, true, false, false, true, true, true,
                false, false, false, true, true, false, true, true, false, true, false, true,
            ],
            vec![
                false, true, true, false, false, true, false, false, true, false, false, true,
                true, false, false,
            ],
            vec![false, true, false, false, false, true, false],
            vec![
                true, false, true, false, true, false, false, false, false, true, false, true,
                true, true, true, true, true, false, false, true, false, false, false, true, true,
                false, true, false, false, true, false, true, false, true, true, true, true, true,
                true, true,
            ],
            vec![
                false, false, false, false, false, false, false, false, true, false, true, true,
                true, true, true, true, true, true, false, false, false, false, true, false, false,
                true,
            ],
            vec![false, true, true, false],
            vec![
                true, false, true, false, true, true, true, false, true, true, true, true, true,
                true, true, false, true, false, false, false, true, false, false,
            ],
            vec![
                false, true, false, true, false, false, false, true, false, true, true, false,
                true, false, false, true, false, true, true, false, false, false, false,
            ],
            vec![
                false, true, true, true, false, true, true, false, false, false, false, false,
                false, true, true, true, false, true, true, true, false, true, true, false, false,
                true, true, false, false, true, true, false, true, false, false, true, false,
                false, true, true,
            ],
            vec![
                true, false, false, false, false, true, true, false, false, true, false, true,
                false, true, true, false, true, false, true, true, false, false, true, true, true,
            ],
            vec![
                false, true, false, true, true, true, false, true, false, true, false, false,
                false, true, false, false, false, false, true, false, false, true, false, false,
                false, false, true, true, true, false, true,
            ],
            vec![
                true, false, true, true, false, false, false, true, true, true, true, false, false,
                true, false, true, false, false, true, true, false, true, false, false, true, true,
                false, false,
            ],
            vec![
                true, false, false, false, false, true, false, true, false, true, false, false,
                false, true, false, true, true, false, false, false, true, false, false, true,
            ],
            vec![
                false, false, false, true, true, false, false, false, false, true, true, true,
                false, false, false, false, true, false, false, false,
            ],
            vec![
                false, true, true, false, false, false, true, false, false, false, false, true,
                true, true, false, false, true, true, true, true, true, false, false, false, false,
                false, true, false,
            ],
            vec![
                false, false, false, false, false, true, true, false, false, true, false, false,
                false, true, false, false, true, false,
            ],
            vec![true, false, true],
            vec![
                true, false, false, false, true, true, true, true, true, true, true, true, false,
                false, false, true, true, true, true, false, false, false, true, false, true,
                false, false, true, false, false, false, true, false, false, false, true, false,
                false, true,
            ],
            vec![true, false, true],
            vec![false, true, false, false, false, false],
            vec![
                false, false, true, true, true, true, true, false, true, true, false,
            ],
            vec![
                false, false, true, true, true, true, true, true, false, true,
            ],
            vec![
                true, false, false, true, false, true, false, false, true, true, true, true, false,
                false, false, false, true, true, true, false, false, true, false, false, true,
                false, true, false, false, false, true, true, true, false, false, false, false,
                true, true, false, false, false, false, true, true,
            ],
            vec![
                true, true, true, true, false, true, false, false, true, true, false, false, true,
                false, true, false, false,
            ],
            vec![false, true, true, true, false, true, true, false],
            vec![
                false, true, false, true, false, false, false, true, false, false, false, true,
                false, false, false, false, true, true, false, true, true, true, true, true, false,
                false, true,
            ],
            vec![false, false, true, true],
            vec![
                false, true, true, false, false, false, false, false, false, true, false, false,
                true, false, true, false, true, true, false, true, true, true, false, false, true,
                false, true, false, true, true, false, true, true, false, false, true, false,
                false, true, true, true, false, true, false, true,
            ],
            vec![
                false, true, true, false, false, true, true, false, true, false, true, true, true,
                false, false, true, true, true, false, true, true, true, false, false, true, false,
                true, false, true,
            ],
            vec![false, false, true, true],
            vec![
                true, false, false, false, true, false, true, false, true, true, true, true, true,
                false, false, false, false, false, true, false, false, false, true, true, true,
                false, true, false, true, false, true, true, false,
            ],
            vec![
                true, false, true, false, false, true, true, true, true, false, true, true, true,
                false, false, true, true, true, true, false, true, true, false, true, true, true,
                true, true, true, true,
            ],
            vec![
                true, false, false, true, true, true, true, true, true, false,
            ],
            vec![
                false, true, true, false, true, true, true, false, true, true, true, true, false,
                false, true, false, false, true, false, true, false, true, true, false, true, true,
                false, true, false, false, false, false, false, true, false,
            ],
            vec![
                false, false, false, true, false, false, false, true, true, false, false,
            ],
            vec![
                false, false, false, true, true, false, false, true, false, true, true, true, true,
                true, false, false, false, true, false, true, false, false, true, true,
            ],
            vec![
                false, false, false, true, false, true, true, false, true, true, false, false,
                false, false, true, true, true, true, false, true, true, true, true, false,
            ],
            vec![
                false, true, false, false, false, true, true, false, false, false, false, true,
                false, true, false, false, true, true, false, false, true, true, false, true, true,
                true, false, false, true, true, false, false, false, true, true, false, true,
                false, false, true, false, false, false, false, false, false, false,
            ],
            vec![false, false, false, true, false, true, false, false],
            vec![
                false, true, true, true, true, false, true, false, false, true, true, true, true,
                true, true, false, false, true, false, false, true, true, true, false, true, true,
                true, false, false, true, false, false, false, true, true, false, true, false,
                true, false, true, false, false, true, true,
            ],
            vec![
                true, true, false, true, false, true, true, true, false, true, true, true, false,
            ],
            vec![
                false, true, false, true, true, true, false, true, false, false, false, true,
                false, false, false, false, true, true, false, false, false, true, true, true,
                false, true, false, false, false, false, true, true, false, false, false, true,
                false, true, false, false, false, true, false, false, false, true,
            ],
            vec![
                false, false, true, true, true, false, true, true, true, true, false, true, true,
                true, false, true, true, true, false, true, false, false, false, false, false,
                true, false, true, false,
            ],
            vec![
                true, true, true, false, true, true, false, true, false, false, true, true, true,
                false, false, true, false, false, false, true, true, false, true, false, false,
                false, true, true, true, true, true, false, true,
            ],
            vec![
                false, false, false, false, true, false, false, false, false, false, false,
            ],
            vec![
                true, false, true, true, false, true, true, false, true, false, true, true, true,
                false, true, false, true, true, true, true, true, true, true, true, true, false,
                true, true, false, false, true,
            ],
            vec![
                true, true, false, true, false, false, true, false, true, true, true, false,
            ],
            vec![
                true, false, true, true, true, true, true, false, true, true, false, false, true,
                false, true, false, true, false, true, false, true, true, false, true, false, true,
                false, true, false, true, false, false, true, false, true, true, true, false, true,
            ],
            vec![
                true, true, true, true, true, false, false, false, false, false, false, true, true,
                false, true, true, true, true,
            ],
            vec![
                false, false, true, false, true, false, true, true, false, false, true, false,
                false, true, true, false, true, false, true, false, true, true, true, true, true,
                false, false, false, false, true,
            ],
            vec![
                true, true, false, false, false, false, true, false, true, false, false, false,
                true, true, false, false, true, false, false, true, false, false, true, true,
                false, false, false, true, false, false, true, true, false, false, false, false,
                false, false, true, false, true, true,
            ],
            vec![
                false, false, false, false, false, true, false, false, false, false, true, true,
                false, true, true, false, true, false, true, false, true, true, false, true, false,
                true, true,
            ],
            vec![
                false, true, false, false, false, false, false, false, false, false, true, false,
                true, false, true, false, false, false, true, true, false, true, false, true, true,
                false, true, false, true, false, true, true, true, true,
            ],
            vec![true, true, false, false, false, false, true, false],
            vec![
                false, false, false, true, true, true, true, true, false, false, false, false,
                false, false, false, true, false, true, false, true, false, false, true, true,
                false, true, false, false, true, true, false, false, true, true, false, false,
                false, false, false, false, true,
            ],
            vec![
                false, true, true, false, true, false, true, false, false, true, false, false,
                false, false, false, true, true, true, true, false, false, true, false, false,
                false, false, false, true, true, true, false, false, true, true, true, true, false,
                false, true, true, false, true, true, true, true, true,
            ],
            vec![
                true, false, true, true, false, true, true, true, true, false, true, true, false,
                false, false, false, true, true, false, true, true, false, true, true, false,
                false, true, false, true, false,
            ],
            vec![false, true, true],
            vec![
                false, false, true, true, false, false, false, false, true, true, true, true, true,
                true, false, true, true, true,
            ],
        ];

        for values in data {
            let mut enc = BooleanEncoder::new(1024);

            for v in &values {
                enc.write(*v);
            }

            // Retrieve compressed bytes.
            let b = enc.bytes().unwrap();

            // Read values out of decoder.
            let mut got = Vec::with_capacity(values.len());

            let mut dec = BooleanDecoder::new(b.as_slice()).unwrap();
            while dec.next() {
                got.push(dec.read());
            }

            assert_eq!(got.len(), values.len());
            for i in 0..got.len() {
                assert_eq!(got[i], values[i]);
            }
        }
    }
}
