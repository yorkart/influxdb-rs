use crate::estimator::hll::compressed::CompressedList;
use std::collections::HashMap;

/// Current version of HLL implementation.
const version: u8 = 2;

/// DEFAULT_PRECISION is the default precision.
const DEFAULT_PRECISION: usize = 16;

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
    tmp_set: HashMap<u32, bool>,

    /// The dense representation of the HLL.
    dense_list: Vec<u8>,
    /// values that can be stored in the sparse representation.
    sparse_list: CompressedList,
}
