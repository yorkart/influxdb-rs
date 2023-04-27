use crate::estimator::hll::compressed::{CompressedList, Iterable};
use crate::estimator::Sketch;
use influxdb_utils::bits;
use influxdb_utils::rhh::hash_key;
use std::collections::{HashMap, HashSet};
use std::mem;

/// Current version of HLL implementation.
const VERSION: u8 = 2;

/// DEFAULT_PRECISION is the default precision.
const DEFAULT_PRECISION: u8 = 16;

fn beta(ez: f64) -> f64 {
    let zl = f64::ln(ez + 1_f64);
    -0.37331876643753059 * ez
        + -1.41704077448122989 * zl
        + 0.40729184796612533 * f64::powi(zl, 2)
        + 1.56152033906584164 * f64::powi(zl, 3)
        + -0.99242233534286128 * f64::powi(zl, 4)
        + 0.26064681399483092 * f64::powi(zl, 5)
        + -0.03053811369682807 * f64::powi(zl, 6)
        + 0.00155770210179105 * f64::powi(zl, 7)
}

/// Plus implements the Hyperloglog++ algorithm, described in the following
/// paper: http://static.googleusercontent.com/media/research.google.com/en//pubs/archive/40671.pdf
///
/// The HyperLogLog++ algorithm provides cardinality estimations.
pub struct Plus {
    /// precision.
    p: u8,
    /// p' (sparse) precision to be used when p ∈ [4..pp] and pp < 64.
    pp: u8,

    /// Number of substream used for stochastic averaging of stream.
    m: u32,
    /// m' (sparse) number of substreams.
    mp: u32,

    /// alpha is used for bias correction.
    alpha: f64,

    /// Should we use a sparse sketch representation.
    sparse: bool,
    tmp_set: HashSet<u32>,

    /// The dense representation of the HLL.
    dense_list: Vec<u8>,
    /// values that can be stored in the sparse representation.
    sparse_list: CompressedList,
}

impl Plus {
    pub fn new() -> anyhow::Result<Self> {
        Self::with_p(DEFAULT_PRECISION)
    }

    pub fn with_p(p: u8) -> anyhow::Result<Self> {
        if p > 18 || p < 4 {
            return Err(anyhow!("precision must be between 4 and 18"));
        }

        // p' = 25 is used in the Google paper.
        let pp = 25_u8;

        let m = 1 << p;
        let mp = 1 << pp;

        let alpha = match m {
            16 => 0.673,
            32 => 0.697,
            64 => 0.709,
            _ => 0.7213 / (1_f64 + 1.079 / (m as f64)),
        };

        Ok(Self {
            p,
            pp,
            m,
            mp,
            alpha,
            sparse: true,
            tmp_set: HashSet::new(),
            dense_list: Vec::new(),
            sparse_list: CompressedList::new(m as usize),
        })
    }

    /// Bytes estimates the memory footprint of this Plus, in bytes.
    pub fn bytes(&self) -> usize {
        let mut b = 0;
        b += self.tmp_set.len() * 4;
        b += self.dense_list.capacity();
        if self.sparse_list.len() > 0 {
            b += 16; // int(unsafe.Sizeof(*h.sparseList))
            b += self.sparse_list.capacity();
        }
        b += mem::size_of::<Self>();
        b
    }

    /// Encode a hash to be used in the sparse representation.
    fn encodeHash(&self, x: u64) -> u32 {
        let idx = bextr(x, (64 - self.pp) as u8, self.pp as u8) as u32;
        if bextr(x, (64 - self.pp) as u8, (self.pp - self.p) as u8) == 0 {
            let zeros = bits::LeadingZeros64(
                bextr(x, 0_u8, (64 - self.pp) as u8) << self.pp | (1 << self.pp - 1),
            ) + 1;
            return idx << 7 | ((zeros << 1) as u32) | 1;
        }
        idx << 1
    }
}

impl Sketch for Plus {
    fn add(&mut self, v: &[u8]) {
        let x = hash_key(v);
        if self.sparse {
            self.tmp_set.insert(self.encodeHash(x));
            if (self.tmp_set.len() * 100) as u32 > self.m {}
        }
    }

    fn count(&self) -> u64 {
        todo!()
    }

    fn merge<T: Sketch>(&mut self, s: T) -> anyhow::Result<()> {
        todo!()
    }

    fn bytes(&self) -> usize {
        todo!()
    }

    fn encode(&self) -> Vec<u8> {
        todo!()
    }
}

/// bextr performs a bitfield extract on v. start should be the LSB of the field
/// you wish to extract, and length the number of bits to extract.
///
/// For example: start=0 and length=4 for the following 64-bit word would result
/// in 1111 being returned.
///
/// <snip 56 bits>00011110
/// returns 1110
fn bextr(v: u64, start: u8, length: u8) -> u64 {
    (v >> start) & ((1 << length) - 1)
}

fn bextr32(v: u32, start: u8, length: u8) -> u32 {
    (v >> start) & ((1 << length) - 1)
}
