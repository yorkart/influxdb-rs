use std::{error, fmt};

/// Bit
///
/// An enum used to represent a single bit, can be either `Zero` or `One`.
#[derive(Debug, PartialEq)]
pub enum Bit {
    Zero,
    One,
}

impl Bit {
    /// Convert a bit to u64, so `Zero` becomes 0 and `One` becomes 1.
    pub fn to_u64(&self) -> u64 {
        match self {
            Bit::Zero => 0,
            Bit::One => 1,
        }
    }
}

/// Error
///
/// Enum used to represent potential errors when interacting with a stream.
#[derive(Debug, PartialEq)]
pub enum Error {
    EOF,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::EOF => write!(f, "Encountered the end of the stream"),
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::EOF => "Encountered the end of the stream",
        }
    }
}

/// Read
///
/// Read is a trait that encapsulates the functionality required to read from a stream of bytes.
pub trait Read {
    /// Read a single bit from the underlying stream.
    fn read_bit(&mut self) -> Result<Bit, Error>;

    /// Read a single byte from the underlying stream.
    fn read_byte(&mut self) -> Result<u8, Error>;

    /// Read `num` bits from the underlying stream.
    fn read_bits(&mut self, num: u32) -> Result<u64, Error>;

    /// Get the next `num` bits, but do not update place in stream.
    fn peak_bits(&mut self, num: u32) -> Result<u64, Error>;
}

/// Write
///
/// Write is a trait that encapsulates the functionality required to write a stream of bytes.
pub trait Write {
    // Write a single bit to the underlying stream.
    fn write_bit(&mut self, bit: Bit);

    // Write a single byte to the underlying stream.
    fn write_byte(&mut self, byte: u8);

    // Write the bottom `num` bits of `bits` to the underlying stream.
    fn write_bits(&mut self, bits: u64, num: u32);

    // Close the underlying stream and return a pointer to the array of bytes.
    fn close(self) -> Box<[u8]>;
}

/// BufferedReader
///
/// BufferedReader encapsulates a buffer of bytes which can be read from.
#[derive(Debug)]
pub struct BufferedReader<'a> {
    bytes: &'a [u8], // internal buffer of bytes
    index: usize,    // index into bytes
    pos: u32,        // position in the byte we are currenlty reading
}

impl<'a> BufferedReader<'a> {
    /// new creates a new `BufferedReader` from `bytes`
    pub fn new(bytes: &'a [u8]) -> Self {
        BufferedReader {
            bytes,
            index: 0,
            pos: 0,
        }
    }

    fn get_byte(&self) -> Result<u8, Error> {
        if self.index >= self.bytes.len() {
            Err(Error::EOF)
        } else {
            Ok(self.bytes[self.index])
        }
    }
}

impl<'a> Read for BufferedReader<'a> {
    fn read_bit(&mut self) -> Result<Bit, Error> {
        if self.pos == 8 {
            self.index += 1;
            self.pos = 0;
        }

        let byte = self.get_byte()?;

        let bit = if byte & 1u8.wrapping_shl(7 - self.pos) == 0 {
            Bit::Zero
        } else {
            Bit::One
        };

        self.pos += 1;

        Ok(bit)
    }

    fn read_byte(&mut self) -> Result<u8, Error> {
        if self.pos == 0 {
            self.pos += 8;
            return self.get_byte();
        }

        if self.pos == 8 {
            self.index += 1;
            return self.get_byte();
        }

        let mut byte = 0;
        let mut b = self.get_byte()?;

        byte |= b.wrapping_shl(self.pos);

        self.index += 1;
        b = self.get_byte()?;

        byte |= b.wrapping_shr(8 - self.pos);

        Ok(byte)
    }

    fn read_bits(&mut self, mut num: u32) -> Result<u64, Error> {
        // can't read more than 64 bits into a u64
        if num > 64 {
            num = 64;
        }

        let mut bits: u64 = 0;
        while num >= 8 {
            let byte = self.read_byte().map(u64::from)?;
            bits = bits.wrapping_shl(8) | byte;
            num -= 8;
        }

        while num > 0 {
            self.read_bit()
                .map(|bit| bits = bits.wrapping_shl(1) | bit.to_u64())?;

            num -= 1;
        }

        Ok(bits)
    }

    fn peak_bits(&mut self, num: u32) -> Result<u64, Error> {
        // save the current index and pos so we can reset them after calling `read_bits`
        let index = self.index;
        let pos = self.pos;

        let bits = self.read_bits(num)?;

        self.index = index;
        self.pos = pos;

        Ok(bits)
    }
}

/// BufferedWriter
///
/// BufferedWriter writes bytes to a buffer.
#[derive(Debug, Default)]
pub struct BufferedWriter {
    buf: Vec<u8>,
    pos: u32, // position in the last byte in the buffer
}

impl BufferedWriter {
    /// new creates a new BufferedWriter
    pub fn new() -> Self {
        BufferedWriter {
            buf: Vec::new(),
            // set pos to 8 to indicate the buffer has no space presently since it is empty
            pos: 8,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        BufferedWriter {
            buf: Vec::with_capacity(capacity),
            // set pos to 8 to indicate the buffer has no space presently since it is empty
            pos: 8,
        }
    }

    fn grow(&mut self) {
        self.buf.push(0);
    }

    fn last_index(&self) -> usize {
        self.buf.len() - 1
    }

    pub fn as_slice(&self) -> &[u8] {
        self.buf.as_slice()
    }
}

impl Write for BufferedWriter {
    fn write_bit(&mut self, bit: Bit) {
        if self.pos == 8 {
            self.grow();
            self.pos = 0;
        }

        let i = self.last_index();

        match bit {
            Bit::Zero => (),
            Bit::One => self.buf[i] |= 1u8.wrapping_shl(7 - self.pos),
        };

        self.pos += 1;
    }

    fn write_byte(&mut self, byte: u8) {
        if self.pos == 8 {
            self.grow();

            let i = self.last_index();
            self.buf[i] = byte;
            return;
        }

        let i = self.last_index();
        let mut b = byte.wrapping_shr(self.pos);
        self.buf[i] |= b;

        self.grow();

        b = byte.wrapping_shl(8 - self.pos);
        self.buf[i + 1] |= b;
    }

    fn write_bits(&mut self, mut bits: u64, mut num: u32) {
        // we should never write more than 64 bits for a u64
        if num > 64 {
            num = 64;
        }

        bits = bits.wrapping_shl(64 - num);
        while num >= 8 {
            let byte = bits.wrapping_shr(56);
            self.write_byte(byte as u8);

            bits = bits.wrapping_shl(8);
            num -= 8;
        }

        while num > 0 {
            let byte = bits.wrapping_shr(63);
            if byte == 1 {
                self.write_bit(Bit::One);
            } else {
                self.write_bit(Bit::Zero);
            }

            bits = bits.wrapping_shl(1);
            num -= 1;
        }
    }

    fn close(self) -> Box<[u8]> {
        self.buf.into_boxed_slice()
    }
}
