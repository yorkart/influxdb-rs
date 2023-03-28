//! This code is originally from: https://github.com/dgryski/go-tsz and has been modified to remove
//! the timestamp compression fuctionality.
//!
//! It implements the float compression as presented in: http://www.vldb.org/pvldb/vol8/p1816-teller.pdf.
//! This implementation uses a sentinel value of NaN which means that float64 NaN cannot be stored using
//! this version.

use crate::engine::tsm1::encoding::bit_io::Bit::{One, Zero};
use crate::engine::tsm1::encoding::bit_io::{BufferedReader, BufferedWriter, Write};
use anyhow::anyhow;

/// Note: an uncompressed format is not yet implemented.
/// FLOAT_COMPRESSED_GORILLA is a compressed format using the gorilla paper encoding
const FLOAT_COMPRESSED_GORILLA: u8 = 1;

/// uvnan is the constant returned from math.NaN().
const uvnan: u64 = 0x7FF8000000000001;

// same as ^uint64(0) in go
const BASIC_VALUE: u64 = 18446744073709551615;

/// FloatEncoder encodes multiple float64s into a byte slice.
pub struct FloatEncoder {
    val: f64,
    err: Option<anyhow::Error>,

    leading: u64,
    trailing: u64,

    bw: BufferedWriter,

    first: bool,
    finished: bool,
}

impl FloatEncoder {
    pub fn new() -> Self {
        let mut bw = BufferedWriter::new();
        bw.write_byte(FLOAT_COMPRESSED_GORILLA << 4);

        Self {
            val: 0f64,
            err: None,
            leading: 0,
            trailing: BASIC_VALUE,
            bw: BufferedWriter::new(),
            first: true,
            finished: false,
        }
    }

    /// Bytes returns a copy of the underlying byte buffer used in the encoder.
    pub fn bytes(&mut self) -> Vec<u8> {
        self.bw.as_slice().to_vec()
    }

    /// Flush indicates there are no more values to encode.
    pub fn flush(&mut self) {
        if !self.finished {
            // write an end-of-stream record
            self.finished = true;
            self.write(f64::NAN);
            // self.bw.flush(Zero);
        }
    }

    pub fn write(&mut self, v: f64) {
        // Only allow NaN as a sentinel value
        if v.is_nan() && !self.finished {
            self.err = Some(anyhow!("unsupported value: NaN"));
            return;
        }
        if self.first {
            // first point
            self.val = v;
            self.first = false;
            self.bw.write_bits(v.to_bits(), 64);
            return;
        }

        let v_delta = v.to_bits() ^ self.val.to_bits();

        if v_delta == 0 {
            self.bw.write_bit(Zero);
        } else {
            self.bw.write_bit(One);

            let mut leading = v_delta.leading_zeros() as u64; // uint64(bits.LeadingZeros64(v_delta));
            let trailing = v_delta.trailing_zeros() as u64; // uint64(bits.TrailingZeros64(v_delta));

            // Clamp number of leading zeros to avoid overflow when encoding
            leading &= 0x1F;
            if leading >= 32 {
                leading = 31;
            }

            // TODO(dgryski): check if it's 'cheaper' to reset the leading/trailing bits instead
            if self.leading != BASIC_VALUE && leading >= self.leading && trailing >= self.trailing {
                self.bw.write_bit(Zero);
                self.bw.write_bits(
                    v_delta >> self.trailing,
                    64 - (self.leading as u32) - (self.trailing as u32),
                )
            } else {
                self.leading = leading;
                self.trailing = trailing;

                self.bw.write_bit(One);
                self.bw.write_bits(leading, 5);

                // Note that if leading == trailing == 0, then sigbits == 64.  But that
                // value doesn't actually fit into the 6 bits we have.
                // Luckily, we never need to encode 0 significant bits, since that would
                // put us in the other case (vdelta == 0).  So instead we write out a 0 and
                // adjust it back to 64 on unpacking.
                let sigbits = 64 - leading - trailing;
                self.bw.write_bits(sigbits, 6);
                self.bw.write_bits(v_delta >> trailing, sigbits as u32);
            }
        }

        self.val = v;
    }
}

/// FloatDecoder decodes a byte slice into multiple float64 values.
pub struct FloatDecoder {
    val: f64,
    err: Option<anyhow::Error>,

    leading: u64,
    trailing: u64,

    br: BufferedReader,

    first: bool,
    finished: bool,
}
