//! String encoding uses snappy compression to compress each string.  Each string is
//! appended to byte slice prefixed with a variable byte length followed by the string
//! bytes.  The bytes are compressed using snappy compressor and a 1 byte header is used
//! to indicate the type of encoding.

//! Note: an uncompressed format is not yet implemented.

use std::sync::Arc;

use crate::engine::tsm1::codec::varint::VarInt;
use crate::engine::tsm1::codec::{Decoder, Encoder};

/// STRING_COMPRESSED_SNAPPY is a compressed encoding using Snappy compression
const STRING_COMPRESSED_SNAPPY: u8 = 1;

/// StringEncoder encodes multiple strings into a byte slice.
pub struct StringEncoder {
    // The encoded bytes
    bytes: Vec<u8>,
}

impl StringEncoder {
    /// NewStringEncoder returns a new StringEncoder with an initial buffer ready to hold sz bytes.
    pub fn new(sz: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(sz),
        }
    }
}

impl Encoder<Vec<u8>> for StringEncoder {
    fn write(&mut self, v: Vec<u8>) {
        let mut b = [0; 10];

        // Append the length of the string using variable byte encoding
        let i = (v.len() as u64).encode_var(&mut b);
        self.bytes.extend_from_slice(&b[..i]);

        // Append the string bytes
        self.bytes.extend_from_slice(v.as_slice());
    }

    fn flush(&mut self) {}

    fn bytes(&mut self) -> anyhow::Result<Vec<u8>> {
        let max_encoded_len = snap::raw::max_compress_len(self.bytes.len());
        if max_encoded_len == 0 {
            return Err(anyhow!("source length too large"));
        }

        let mut compressed_data = Vec::with_capacity(max_encoded_len + 1);
        compressed_data.resize(max_encoded_len + 1, 0);

        // header
        compressed_data[0] = STRING_COMPRESSED_SNAPPY << 4;

        let mut encoder = snap::raw::Encoder::new();
        let actual_compressed_size = encoder
            .compress(self.bytes.as_slice(), &mut compressed_data[1..])
            .map_err(|e| anyhow!(e))?;

        compressed_data.truncate(1 + actual_compressed_size);
        Ok(compressed_data)
    }
}

/// StringDecoder decodes a byte slice into strings.
pub struct StringDecoder {
    b: Vec<u8>,
    l: usize,
    i: usize,

    lower: usize,
    upper: usize,

    err: Option<anyhow::Error>,
}

impl StringDecoder {
    /// initializes the decoder with bytes to read from.
    /// This must be called before calling any other method.
    pub fn new(b: &[u8]) -> anyhow::Result<Self> {
        if b.len() == 0 {
            return Err(anyhow!("no data found"));
        }

        let mut decoder = snap::raw::Decoder::new();
        // First byte stores the encoding type, only have snappy format
        // currently so ignore for now.
        let decoded_bytes = decoder.decompress_vec(&b[1..]).map_err(|e| anyhow!(e))?;

        Ok(Self {
            b: decoded_bytes,
            l: 0,
            i: 0,
            lower: 0,
            upper: 0,
            err: None,
        })
    }

    /// Read returns the next value from the decoder.
    fn read_range(&mut self) -> anyhow::Result<(usize, usize)> {
        // Read the length of the string
        let r = u64::decode_var(&self.b[self.i..]);
        if r.is_none() {
            return Err(anyhow!("StringDecoder: invalid encoded string length"));
        }

        let (length, n) = r.unwrap();
        if n <= 0 {
            return Err(anyhow!("StringDecoder: invalid encoded string length"));
        }

        // The length of this string plus the length of the variable byte encoded length
        self.l = (length as usize) + n;

        let lower = self.i + n;
        let upper = lower + (length as usize);
        if upper < lower {
            return Err(anyhow!("StringDecoder: length overflow"));
        }
        if upper > self.b.len() {
            return Err(anyhow!(
                "StringDecoder: not enough data to represent encoded string"
            ));
        }

        Ok((lower, upper))
    }

    pub fn read_string(&self) -> anyhow::Result<String> {
        let ref_data = self.read();
        String::from_utf8(ref_data.as_slice().to_vec()).map_err(|e| anyhow!(e))
        // std::str::from_utf8(ref_data.as_slice()).map_err(|e| anyhow!(e))
    }
}

impl Decoder<Vec<u8>> for StringDecoder {
    fn next(&mut self) -> bool {
        if self.err.is_some() {
            return false;
        }

        self.i += self.l;
        let b = self.i < self.b.len();
        if !b {
            return b;
        }

        let r = self.read_range();
        return match r {
            Ok((lower, upper)) => {
                self.lower = lower;
                self.upper = upper;

                true
            }
            Err(e) => {
                self.err = Some(e);
                false
            }
        };
    }

    fn read(&self) -> Vec<u8> {
        self.b[self.lower..self.upper].to_vec()
        // Ref {
        //     buf: self.b.clone(),
        //     lower: self.lower,
        //     upper: self.upper,
        // }
        // & self.b[self.lower..self.upper]
    }

    fn err(&self) -> Option<&anyhow::Error> {
        self.err.as_ref()
    }
}

#[derive(Clone)]
pub struct Ref {
    buf: Arc<Vec<u8>>,
    lower: usize,
    upper: usize,
}

impl Ref {
    pub fn as_slice(&self) -> &[u8] {
        &self.buf.as_ref()[self.lower..self.upper]
    }

    pub fn to_vec(&self) -> Vec<u8> {
        self.as_slice().to_vec()
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::tsm1::codec::string::{
        StringDecoder, StringEncoder, STRING_COMPRESSED_SNAPPY,
    };
    use crate::engine::tsm1::codec::{Decoder, Encoder};

    #[test]
    fn test_string_encoder_no_values() {
        let mut enc = StringEncoder::new(1024);
        let b = enc.bytes().unwrap();

        let mut dec = StringDecoder::new(b.as_slice()).unwrap();
        assert_eq!(
            dec.next(),
            false,
            "unexpected next value: got true, exp false"
        );
    }

    #[test]
    fn test_string_encoder_single() {
        let mut enc = StringEncoder::new(1024);
        let v1 = "v1";
        enc.write(v1.as_bytes().to_vec());

        let b = enc.bytes().unwrap();

        let mut dec = StringDecoder::new(b.as_slice()).unwrap();
        assert_eq!(
            dec.next(),
            true,
            "unexpected next value: got false, exp true"
        );
        assert_eq!(
            dec.read_string().unwrap(),
            v1,
            "unexpected value: got {}, exp {}",
            dec.read_string().unwrap(),
            v1
        )
    }

    #[test]
    fn test_string_encoder_multi_compressed() {
        let mut enc = StringEncoder::new(1024);

        let mut values = Vec::with_capacity(10);
        for i in 0..10 {
            values.push(format!("value {}", i));
            enc.write(values[i].as_bytes().to_vec());
        }

        let b = enc.bytes().unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            got, STRING_COMPRESSED_SNAPPY,
            "unexpected encoding: got {}, exp {}",
            b[0], STRING_COMPRESSED_SNAPPY
        );

        let exp = 51;
        assert_eq!(
            exp,
            b.len(),
            "unexpected length: got {}, exp {}",
            b.len(),
            exp
        );

        let mut dec = StringDecoder::new(b.as_slice()).unwrap();
        for (i, v) in values.into_iter().enumerate() {
            assert_eq!(
                dec.next(),
                true,
                "unexpected next value: got false, exp true"
            );
            assert_eq!(
                dec.read_string().unwrap(),
                v.as_str(),
                "unexpected value at pos {}: got {}, exp {}",
                i,
                dec.read_string().unwrap(),
                v.as_str()
            );
        }

        assert_eq!(
            dec.next(),
            false,
            "unexpected next value: got true, exp false"
        );
    }

    #[test]
    fn test_string_decoder_empty() {
        let dec_r = StringDecoder::new("".as_bytes());
        assert_eq!(
            dec_r.is_err(),
            true,
            "unexpected next value: got true, exp false"
        );
    }
}
