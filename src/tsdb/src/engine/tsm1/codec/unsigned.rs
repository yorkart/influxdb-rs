use crate::engine::tsm1::codec::integer::{IntegerDecoder, IntegerEncoder};
use crate::engine::tsm1::codec::{Decoder, Encoder};
use anyhow::Error;

pub struct UnsignedEncoder {
    enc: IntegerEncoder,
}

impl UnsignedEncoder {
    pub fn new(sz: usize) -> Self {
        Self {
            enc: IntegerEncoder::new(sz),
        }
    }
}

impl Encoder<u64> for UnsignedEncoder {
    fn write(&mut self, v: u64) {
        self.enc.write(v as i64);
    }

    fn flush(&mut self) {
        self.enc.flush();
    }

    fn bytes(&mut self) -> anyhow::Result<Vec<u8>> {
        self.enc.bytes()
    }
}

pub struct UnsignedDecoder<'a> {
    dec: IntegerDecoder<'a>,
}

impl<'a> UnsignedDecoder<'a> {
    pub fn new(b: &'a [u8]) -> anyhow::Result<Self> {
        IntegerDecoder::new(b).map(|dec| Self { dec })
    }
}

impl<'a> Decoder<u64> for UnsignedDecoder<'a> {
    fn next(&mut self) -> bool {
        self.dec.next()
    }

    fn read(&self) -> u64 {
        self.dec.read() as u64
    }

    fn err(&self) -> Option<&Error> {
        self.dec.err()
    }
}
