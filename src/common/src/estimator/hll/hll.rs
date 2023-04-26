/// Current version of HLL implementation.
const version: u8 = 2;

/// DefaultPrecision is the default precision.
const DefaultPrecision: usize = 16;

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
pub struct Plus {}
